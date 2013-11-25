package redis

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

const (
	MAX_CONNECTIONS = 1024
)

const (
	StatusReply = iota
	NumberReply
	BulkReply
	MultiReply
)

type Reply struct {
	Type   int
	Status string
	Number int64
	Bulk   []byte
	Multi  [][]byte
}

type Redis struct {
	network  string
	address  string
	db       int
	password string
	timeout  time.Duration
	size     int
	pool     chan net.Conn
}

func DialTimeout(network, address string, db int, password string, timeout time.Duration, size int) (*Redis, error) {
	if size < 1 {
		size = 1
	} else if size > MAX_CONNECTIONS {
		size = MAX_CONNECTIONS
	}
	if db < 0 {
		db = 0
	}
	r := &Redis{
		network:  network,
		address:  address,
		db:       db,
		password: password,
		timeout:  timeout,
		size:     size,
		pool:     make(chan net.Conn, size),
	}
	for i := 0; i < size; i++ {
		r.pool <- nil
	}
	conn, err := r.getConnection()
	if err != nil {
		return nil, err
	}
	r.activeConnection(conn)
	return r, nil
}

func (r *Redis) getConnection() (net.Conn, error) {
	c := <-r.pool
	if c == nil {
		return r.openConnection()
	}
	return c, nil
}

func (r *Redis) activeConnection(conn net.Conn) {
	r.pool <- conn
}

func (r *Redis) openConnection() (net.Conn, error) {
	conn, err := net.DialTimeout(r.network, r.address, r.timeout)
	if err != nil {
		return nil, err
	}
	if r.password != "" {
		if err := r.sendConnectionCmd(conn, "AUTH", r.password); err != nil {
			return nil, err
		}
		if _, err := r.recvConnectionReply(conn); err != nil {
			return nil, err
		}
	}
	if r.db > 0 {
		if err := r.sendConnectionCmd(conn, "SELECT", r.db); err != nil {
			return nil, err
		}
		if _, err := r.recvConnectionReply(conn); err != nil {
			return nil, err
		}
	}
	return conn, nil
}

func (r *Redis) sendConnectionCmd(conn net.Conn, args ...interface{}) error {
	cmd, err := r.packCommand(args...)
	if err != nil {
		return err
	}
	if _, err := conn.Write(cmd); err != nil {
		return err
	}
	return nil
}

func (r *Redis) packCommand(args ...interface{}) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	if _, err := fmt.Fprintf(buf, "*%d\r\n", len(args)); err != nil {
		return nil, err
	}
	for _, arg := range args {
		s := fmt.Sprint(arg)
		if _, err := fmt.Fprintf(buf, "$%d\r\n%s\r\n", len(s), s); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func (r *Redis) recvConnectionReply(conn net.Conn) (*Reply, error) {
	reader := bufio.NewReader(conn)
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	line = line[:len(line)-2]
	switch line[0] {
	case '-':
		return nil, errors.New(string(line[1:]))
	case '+':
		return &Reply{
			Type:   StatusReply,
			Status: string(line[1:]),
		}, nil
	case ':':
		i, err := strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return nil, err
		}
		return &Reply{
			Type:   NumberReply,
			Number: i,
		}, nil
	case '$':
		size, err := strconv.Atoi(string(line[1:]))
		if err != nil {
			return nil, err
		}
		bulk, err := r.readSize(reader, size)
		if err != nil {
			return nil, err
		}
		return &Reply{
			Type: BulkReply,
			Bulk: bulk,
		}, nil
	case '*':
		i, err := strconv.Atoi(string(line[1:]))
		if err != nil {
			return nil, err
		}
		multi := make([][]byte, i)
		for j := 0; j < i; j++ {
			bulk, err := r.readBulk(reader)
			if err != nil {
				return nil, err
			}
			multi[j] = bulk
		}
		return &Reply{
			Type:  MultiReply,
			Multi: multi,
		}, nil
	}
	return nil, errors.New("redis protocol error")
}

func (r *Redis) readBulk(reader *bufio.Reader) ([]byte, error) {
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if line[0] != '$' {
		return nil, errors.New("not bulk head prefix")
	}
	size, err := strconv.Atoi(string(line[1 : len(line)-2]))
	if err != nil {
		return nil, err
	}
	return r.readSize(reader, size)
}

func (r *Redis) readSize(reader *bufio.Reader, size int) ([]byte, error) {
	if size < 0 {
		return nil, nil
	}
	buf := make([]byte, size+2)
	if _, err := reader.Read(buf); err != nil {
		return nil, err
	}
	return buf[:size], nil
}

func (r *Redis) Close() {
	for i := 0; i < r.size; i++ {
		conn := <-r.pool
		if conn != nil {
			conn.Close()
		}
	}
}

func (r *Redis) sendCommand(args ...interface{}) (*Reply, error) {
	conn, err := r.getConnection()
	defer r.activeConnection(conn)
	if err != nil {
		return nil, err
	}
	if err := r.sendConnectionCmd(conn, args...); err != nil {
		if err == io.EOF {
			conn, err = r.openConnection()
			if err != nil {
				return nil, err
			}
		}
		return nil, err
	}
	return r.recvConnectionReply(conn)
}

func (r *Redis) integerReturnValue(rp *Reply) int64 {
	return rp.Number
}

func (r *Redis) booleanReturnValue(rp *Reply) bool {
	return rp.Number != 0
}

func (r *Redis) okStatusReturnValue(rp *Reply) error {
	if rp.Status == "OK" {
		return nil
	}
	return errors.New(rp.Status)
}

func (r *Redis) statusReturnValue(rp *Reply) string {
	return rp.Status
}

func (r *Redis) bulkReturnValue(rp *Reply) string {
	if rp.Bulk == nil {
		return ""
	}
	return string(rp.Bulk)
}

func (r *Redis) multiBulkReturnValue(rp *Reply) [][]byte {
	return rp.Multi
}

func (r *Redis) hashReturnValue(rp *Reply) map[string]string {
	if rp.Multi == nil {
		return nil
	}
	result := make(map[string]string)
	length := len(rp.Multi)
	for i := 0; i < length/2; i++ {
		var key, value string
		key = string(rp.Multi[i*2])
		if rp.Multi[i*2+1] != nil {
			value = string(rp.Multi[i*2+1])
		}
		result[key] = value
	}
	return result
}

func (r *Redis) listReturnValue(rp *Reply) []string {
	if rp.Multi == nil {
		return nil
	}
	var result []string
	for _, item := range rp.Multi {
		if item == nil {
			result = append(result, "")
		} else {
			result = append(result, string(item))
		}
	}
	return result
}

func (r *Redis) Append(key, value string) (int64, error) {
	rp, err := r.sendCommand("APPEND", key, value)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp), nil
}

func (r *Redis) Auth(password string) error {
	rp, err := r.sendCommand("AUTH", password)
	if err != nil {
		return err
	}
	return r.okStatusReturnValue(rp)
}

func (r *Redis) BgRewriteAof() error {
	_, err := r.sendCommand("BGREWRITEAOF")
	return err
}

func (r *Redis) BgSave() error {
	_, err := r.sendCommand("BGSAVE")
	return err
}

func (r *Redis) BitCount(key, start, end string) (int64, error) {
	rp, err := r.sendCommand("BITCOUNT", key, start, end)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp), nil
}

func (r *Redis) BitOp(operation, destkey string, keys ...string) (int64, error) {
	args := []interface{}{"BITOP", operation, destkey}
	for _, key := range keys {
		args = append(args, key)
	}
	rp, err := r.sendCommand(args...)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp), nil
}

func (r *Redis) BLPop(keys []string, timeout int) (string, string, error) {
	args := []interface{}{"BLPOP"}
	for _, key := range keys {
		args = append(args, key)
	}
	args = append(args, timeout)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return "", "", err
	}
	return string(rp.Multi[0]), string(rp.Multi[1]), nil
}

func (r *Redis) BRPop(keys []string, timeout int) (string, string, error) {
	args := []interface{}{"BRPOP"}
	for _, key := range keys {
		args = append(args, key)
	}
	args = append(args, timeout)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return "", "", err
	}
	return string(rp.Multi[0]), string(rp.Multi[1]), nil
}

func (r *Redis) BRPopLPush(source, destination string, timeout int) (string, error) {
	args := []interface{}{"BRPOPLPUSH", source, destination, timeout}
	rp, err := r.sendCommand(args...)
	if err != nil {
		return "", err
	}
	if rp.Type == MultiReply {
		return "", nil
	}
	return r.bulkReturnValue(rp), nil
}

func (r *Redis) ClientKill(ip string, port int) error {
	arg := net.JoinHostPort(ip, strconv.Itoa(port))
	rp, err := r.sendCommand("CLIENT", "KILL", arg)
	if err != nil {
		return err
	}
	return r.okStatusReturnValue(rp)
}

func (r *Redis) ClientList() ([]map[string]string, error) {
	rp, err := r.sendCommand("CLIENT", "LIST")
	if err != nil {
		return nil, err
	}
	var result []map[string]string
	bulk := string(r.bulkReturnValue(rp))
	for _, line := range strings.Split(bulk, "\n") {
		item := make(map[string]string)
		for _, field := range strings.Fields(line) {
			val := strings.Split(field, "=")
			item[val[0]] = val[1]
		}
		result = append(result, item)
	}
	return result, nil
}

func (r *Redis) ClientGetName() (string, error) {
	rp, err := r.sendCommand("CLIENT", "GETNAME")
	if err != nil {
		return "", err
	}
	return r.bulkReturnValue(rp), nil
}

func (r *Redis) ClientSetName(name string) error {
	rp, err := r.sendCommand("CLIENT", "SETNAME", name)
	if err != nil {
		return err
	}
	return r.okStatusReturnValue(rp)
}

func (r *Redis) ConfigGet(parameter string) (map[string]string, error) {
	rp, err := r.sendCommand("CONFIG", "GET", parameter)
	if err != nil {
		return nil, err
	}
	return r.hashReturnValue(rp), nil
}

func (r *Redis) ConfigRewrite() error {
	rp, err := r.sendCommand("CONFIG", "REWRITE")
	if err != nil {
		return err
	}
	return r.okStatusReturnValue(rp)
}

func (r *Redis) ConfigSet(parameter, value string) error {
	rp, err := r.sendCommand("CONFIG", "SET")
	if err != nil {
		return err
	}
	return r.okStatusReturnValue(rp)
}

func (r *Redis) ConfigResetStat() error {
	_, err := r.sendCommand("CONFIG", "RESETSTAT")
	return err
}

func (r *Redis) DBSize() (int64, error) {
	rp, err := r.sendCommand("DBSIZE")
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp), nil
}

func (r *Redis) Decr(key string) (int64, error) {
	rp, err := r.sendCommand("DECR", key)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp), nil
}

func (r *Redis) DecrBy(key string, decrement int) (int64, error) {
	rp, err := r.sendCommand("DECRBY", key, decrement)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp), nil
}

func (r *Redis) Del(keys ...string) (int64, error) {
	args := []interface{}{"DEL"}
	for _, key := range keys {
		args = append(args, key)
	}
	rp, err := r.sendCommand(args...)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp), nil
}

func (r *Redis) Dump(key string) ([]byte, error) {
	rp, err := r.sendCommand("DUMP", key)
	if err != nil {
		return nil, err
	}
	return rp.Bulk, nil
}

func (r *Redis) Echo(message string) (string, error) {
	rp, err := r.sendCommand("ECHO", message)
	if err != nil {
		return "", err
	}
	return r.bulkReturnValue(rp), nil
}

func (r *Redis) Eval(script string, keys []string, args []string) (*Reply, error) {
	cmds := []interface{}{"EVAL", script, len(keys)}
	for _, key := range keys {
		cmds = append(cmds, key)
	}
	for _, arg := range args {
		cmds = append(cmds, arg)
	}
	return r.sendCommand(cmds...)
}

func (r *Redis) EvalSha(sha1 string, keys []string, args []string) (*Reply, error) {
	return r.Eval(sha1, keys, args)
}

func (r *Redis) Exists(key string) (bool, error) {
	rp, err := r.sendCommand("EXISTS", key)
	if err != nil {
		return false, err
	}
	return r.booleanReturnValue(rp), nil
}

func (r *Redis) Expire(key string, seconds int) (bool, error) {
	rp, err := r.sendCommand("EXPIRE", key, seconds)
	if err != nil {
		return false, err
	}
	return r.booleanReturnValue(rp), nil
}

func (r *Redis) ExpireAt(key string, timestamp int64) (bool, error) {
	rp, err := r.sendCommand("EXPIREAT", key, timestamp)
	if err != nil {
		return false, err
	}
	return r.booleanReturnValue(rp), nil
}

func (r *Redis) FlushAll() error {
	rp, err := r.sendCommand("FLUSHALL")
	if err != nil {
		return err
	}
	return r.okStatusReturnValue(rp)
}

func (r *Redis) FlushDB() error {
	rp, err := r.sendCommand("FLUSHDB")
	if err != nil {
		return err
	}
	return r.okStatusReturnValue(rp)
}

func (r *Redis) Get(key string) ([]byte, error) {
	rp, err := r.sendCommand("GET", key)
	if err != nil {
		return nil, err
	}
	return rp.Bulk, nil
}

func (r *Redis) GetBit(key string, offset int) (int64, error) {
	rp, err := r.sendCommand("GETBIT", key, offset)
	if err != nil {
		return 0, err
	}
	return rp.Number, nil
}

func (r *Redis) GetRange(key string, start, end int) (string, error) {
	rp, err := r.sendCommand("GETRANGE", start, end)
	if err != nil {
		return "", err
	}
	return r.bulkReturnValue(rp), nil
}

func (r *Redis) GetSet(key, value string) (string, error) {
	rp, err := r.sendCommand("GETSET", key, value)
	if err != nil {
		return "", err
	}
	return r.bulkReturnValue(rp), nil
}

func (r *Redis) Transaction() (*Transaction, error) {
	conn, err := r.getConnection()
	if err != nil {
		return nil, err
	}
	return newTransaction(r, conn)
}

type Transaction struct {
	redis  *Redis
	conn   net.Conn
	queued int
}

func newTransaction(r *Redis, conn net.Conn) (*Transaction, error) {
	t := &Transaction{
		redis: r,
		conn:  conn,
	}
	err := t.multi()
	if err != nil {
		r.activeConnection(conn)
		return nil, err
	}
	return t, nil
}

func (t *Transaction) multi() error {
	if err := t.redis.sendConnectionCmd(t.conn, "MULTI"); err != nil {
		return err
	}
	_, err := t.redis.recvConnectionReply(t.conn)
	return err
}

func (t *Transaction) Discard() error {
	if err := t.redis.sendConnectionCmd(t.conn, "DISCARD"); err != nil {
		return err
	}
	_, err := t.redis.recvConnectionReply(t.conn)
	return err
}

func (t *Transaction) Watch(keys ...string) error {
	args := []interface{}{"WATCH"}
	for _, key := range keys {
		args = append(args, key)
	}
	if err := t.redis.sendConnectionCmd(t.conn, args...); err != nil {
		return err
	}
	_, err := t.redis.recvConnectionReply(t.conn)
	return err
}

func (t *Transaction) UnWatch() error {
	if err := t.redis.sendConnectionCmd(t.conn, "UNWATCH"); err != nil {
		return err
	}
	_, err := t.redis.recvConnectionReply(t.conn)
	return err
}

func (t *Transaction) Exec() ([]interface{}, error) {
	if err := t.redis.sendConnectionCmd(t.conn, "EXEC"); err != nil {
		return nil, err
	}
	result := make([]interface{}, t.queued)
	for i := 0; i < t.queued; i++ {
		rp, err := t.redis.recvConnectionReply(t.conn)
		if err != nil {
			result = append(result, err)
		} else {
			result = append(result, rp)
		}
	}
	return result, nil
}

func (t *Transaction) Close() {
	t.redis.activeConnection(t.conn)
}

func (t *Transaction) Command(args ...interface{}) error {
	if err := t.redis.sendConnectionCmd(t.conn, args...); err != nil {
		return err
	}
	rp, err := t.redis.recvConnectionReply(t.conn)
	if err != nil {
		return err
	}
	if rp.Status != "QUEUED" {
		return errors.New(rp.Status)
	}
	t.queued++
	return nil
}
