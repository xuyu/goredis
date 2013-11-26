package redis

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"reflect"
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

func DialURL(rawurl string) (*Redis, error) {
	ul, err := url.Parse(rawurl)
	if err != nil {
		return nil, err
	}
	if strings.ToLower(ul.Scheme) != "redis" {
		return nil, errors.New("invalid scheme")
	}
	network := "tcp"
	address := ul.Host
	db := 0
	password := ""
	timeout := 15 * time.Second
	size := 0
	path := strings.TrimPrefix(ul.Path, "/")
	if pw, set := ul.User.Password(); set {
		password = pw
	}
	if number, err := strconv.Atoi(path); err == nil {
		db = number
	}
	if duration, err := time.ParseDuration(ul.Query().Get("timeout")); err == nil {
		timeout = duration
	}
	if number, err := strconv.Atoi(ul.Query().Get("size")); err == nil {
		size = number
	}
	return DialTimeout(network, address, db, password, timeout, size)
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

func (r *Redis) packArgs(items ...interface{}) (args []interface{}) {
	for _, item := range items {
		v := reflect.ValueOf(item)
		switch v.Kind() {
		case reflect.Slice:
			if v.Elem().Kind() == reflect.Uint8 {
				args = append(args, string(v.Bytes()))
			} else {
				for i := 0; i < v.Len(); i++ {
					args = append(args, v.Index(i).Interface())
				}
			}
		case reflect.Map:
			for _, key := range v.MapKeys() {
				value := v.MapIndex(key)
				args = append(args, key.Interface(), value.Interface())
			}
		case reflect.String:
			if v.String() != "" {
				args = append(args, v.Interface())
			}
		default:
			args = append(args, v.Interface())
		}
	}
	return args
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

func (r *Redis) stringBulkReturnValue(rp *Reply) string {
	if rp.Bulk == nil {
		return ""
	}
	return string(rp.Bulk)
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
	args := r.packArgs("BITOP", operation, destkey, keys)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp), nil
}

func (r *Redis) BLPop(keys []string, timeout int) (string, string, error) {
	args := r.packArgs("BLPOP", keys, timeout)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return "", "", err
	}
	return string(rp.Multi[0]), string(rp.Multi[1]), nil
}

func (r *Redis) BRPop(keys []string, timeout int) (string, string, error) {
	args := r.packArgs("BRPOP", keys, timeout)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return "", "", err
	}
	return string(rp.Multi[0]), string(rp.Multi[1]), nil
}

func (r *Redis) BRPopLPush(source, destination string, timeout int) (string, error) {
	rp, err := r.sendCommand("BRPOPLPUSH", source, destination, timeout)
	if err != nil {
		return "", err
	}
	if rp.Type == MultiReply {
		return "", nil
	}
	return r.stringBulkReturnValue(rp), nil
}

func (r *Redis) ClientKill(ip string, port int) error {
	rp, err := r.sendCommand("CLIENT", "KILL", net.JoinHostPort(ip, strconv.Itoa(port)))
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
	bulk := r.stringBulkReturnValue(rp)
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
	return r.stringBulkReturnValue(rp), nil
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
	args := r.packArgs("DEL", keys)
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
	return r.stringBulkReturnValue(rp), nil
}

func (r *Redis) Eval(script string, keys []string, args []string) (*Reply, error) {
	cmds := r.packArgs("EVAL", script, len(keys), keys, args)
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
	return r.stringBulkReturnValue(rp), nil
}

func (r *Redis) GetSet(key, value string) (string, error) {
	rp, err := r.sendCommand("GETSET", key, value)
	if err != nil {
		return "", err
	}
	return r.stringBulkReturnValue(rp), nil
}

func (r *Redis) HDel(key string, fields ...string) (int64, error) {
	args := r.packArgs("HDEL", key, fields)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp), nil
}

func (r *Redis) HExists(key, field string) (bool, error) {
	rp, err := r.sendCommand("HEXISTS", key, field)
	if err != nil {
		return false, err
	}
	return r.booleanReturnValue(rp), nil
}

func (r *Redis) HGet(key, field string) ([]byte, error) {
	rp, err := r.sendCommand("HGET", key, field)
	if err != nil {
		return nil, err
	}
	return rp.Bulk, nil
}

func (r *Redis) HGetAll(key string) ([]string, error) {
	rp, err := r.sendCommand("HGETALL", key)
	if err != nil {
		return nil, err
	}
	return r.listReturnValue(rp), nil
}

func (r *Redis) HIncrBy(key, field string, increment int) (int64, error) {
	rp, err := r.sendCommand("HINCRBY", key, field, increment)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp), nil
}

func (r *Redis) HIncrByFloat(key, field string, increment float64) (float64, error) {
	rp, err := r.sendCommand("HINCRBYFLOAT", key, field, increment)
	if err != nil {
		return 0, err
	}
	return strconv.ParseFloat(string(rp.Bulk), 64)
}

func (r *Redis) HKeys(key string) ([]string, error) {
	rp, err := r.sendCommand("HKEYS", key)
	if err != nil {
		return nil, err
	}
	return r.listReturnValue(rp), nil
}

func (r *Redis) HLen(key string) (int64, error) {
	rp, err := r.sendCommand("HLEN", key)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp), nil
}

func (r *Redis) HMGet(key string, fields ...string) ([][]byte, error) {
	args := r.packArgs("HMGET", key, fields)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return nil, err
	}
	return rp.Multi, nil
}

func (r *Redis) HMSet(key string, pairs map[string]string) error {
	args := r.packArgs("HMSET", key, pairs)
	rp, err := r.sendCommand(args)
	if err != nil {
		return err
	}
	return r.okStatusReturnValue(rp)
}

func (r *Redis) HSet(key, field, value string) (bool, error) {
	rp, err := r.sendCommand("HSET", key, field, value)
	if err != nil {
		return false, err
	}
	return r.booleanReturnValue(rp), nil
}

func (r *Redis) HSetnx(key, field, value string) (bool, error) {
	rp, err := r.sendCommand("HSETNX", key, field, value)
	if err != nil {
		return false, err
	}
	return r.booleanReturnValue(rp), nil
}

func (r *Redis) HVals(key string) ([]string, error) {
	rp, err := r.sendCommand("HVALS", key)
	if err != nil {
		return nil, err
	}
	return r.listReturnValue(rp), nil
}

func (r *Redis) Incr(key string) (int64, error) {
	rp, err := r.sendCommand("INCR", key)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp), nil
}

func (r *Redis) IncrBy(key string, increment int) (int64, error) {
	rp, err := r.sendCommand("INCRBY", key, increment)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp), nil
}

func (r *Redis) IncrByFloat(key string, increment float64) (float64, error) {
	rp, err := r.sendCommand("INCRBYFLOAT", key, increment)
	if err != nil {
		return 0, err
	}
	return strconv.ParseFloat(string(rp.Bulk), 64)
}

func (r *Redis) Info(section string) (string, error) {
	args := r.packArgs("INFO", section)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return "", err
	}
	return r.stringBulkReturnValue(rp), nil
}

func (r *Redis) Keys(pattern string) ([]string, error) {
	rp, err := r.sendCommand("KEYS", pattern)
	if err != nil {
		return nil, err
	}
	return r.listReturnValue(rp), nil
}

func (r *Redis) LastSave() (int64, error) {
	rp, err := r.sendCommand("LASTSAVE")
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp), nil
}

func (r *Redis) LIndex(key string, index int) ([]byte, error) {
	rp, err := r.sendCommand("LINDEX", key, index)
	if err != nil {
		return nil, err
	}
	return rp.Bulk, nil
}

func (r *Redis) LInsertBefore(key, pivot, value string) (int64, error) {
	rp, err := r.sendCommand("LINSERT", key, "BEFORE", pivot, value)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp), nil
}

func (r *Redis) LInsertAfter(key, pivot, value string) (int64, error) {
	rp, err := r.sendCommand("LINSERT", key, "AFTER", pivot, value)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp), nil
}

func (r *Redis) LLen(key string) (int64, error) {
	rp, err := r.sendCommand("LLEN", key)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp), nil
}

func (r *Redis) LPop(key string) ([]byte, error) {
	rp, err := r.sendCommand("LPOP", key)
	if err != nil {
		return nil, err
	}
	return rp.Bulk, nil
}

func (r *Redis) LPush(key string, values ...string) (int64, error) {
	args := r.packArgs("LPUSH", key, values)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp), nil
}

func (r *Redis) LPushx(key, value string) (int64, error) {
	rp, err := r.sendCommand("LPUSHX", key, value)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp), nil
}

func (r *Redis) LRange(key string, start, end int) ([]string, error) {
	rp, err := r.sendCommand("LRANGE", key, start, end)
	if err != nil {
		return nil, err
	}
	return r.listReturnValue(rp), nil
}

func (r *Redis) LRem(key string, count int, value string) (int64, error) {
	rp, err := r.sendCommand("LREM", key, count, value)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp), nil
}

func (r *Redis) LSet(key string, index int, value string) error {
	rp, err := r.sendCommand("LSET", key, index, value)
	if err != nil {
		return err
	}
	return r.okStatusReturnValue(rp)
}

func (r *Redis) LTrim(key string, start, stop int) error {
	rp, err := r.sendCommand("LTRIM", key, start, stop)
	if err != nil {
		return err
	}
	return r.okStatusReturnValue(rp)
}

func (r *Redis) MGet(keys ...string) ([][]byte, error) {
	args := r.packArgs("MGET", keys)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return nil, err
	}
	return rp.Multi, nil
}

func (r *Redis) Move(key string, db int) (bool, error) {
	rp, err := r.sendCommand("MOVE", key, db)
	if err != nil {
		return false, err
	}
	return r.booleanReturnValue(rp), nil
}

func (r *Redis) MSet(pairs map[string]string) error {
	args := r.packArgs("MSET", pairs)
	_, err := r.sendCommand(args...)
	return err
}

func (r *Redis) MSetnx(pairs map[string]string) (bool, error) {
	args := r.packArgs("MSETNX", pairs)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return false, err
	}
	return r.booleanReturnValue(rp), nil
}

func (r *Redis) Persist(key string) (bool, error) {
	rp, err := r.sendCommand("PERSIST", key)
	if err != nil {
		return false, err
	}
	return r.booleanReturnValue(rp), nil
}

func (r *Redis) PExpire(key string, milliseconds int) (bool, error) {
	rp, err := r.sendCommand("PEXPIRE", key, milliseconds)
	if err != nil {
		return false, err
	}
	return r.booleanReturnValue(rp), nil
}

func (r *Redis) PExpireAt(key string, timestamp int64) (bool, error) {
	rp, err := r.sendCommand("PEXPIREAT", key, timestamp)
	if err != nil {
		return false, err
	}
	return r.booleanReturnValue(rp), nil
}

func (r *Redis) Ping() error {
	rp, err := r.sendCommand("PING")
	if err != nil {
		return err
	}
	if rp.Status != "PONG" {
		return errors.New(rp.Status)
	}
	return nil
}

func (r *Redis) PSetex(key string, milliseconds int, value string) error {
	_, err := r.sendCommand("PSETEX", key, milliseconds, value)
	return err
}

func (r *Redis) PTTL(key string) (int64, error) {
	rp, err := r.sendCommand("PTTL", key)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp), nil
}

func (r *Redis) Quit() error {
	_, err := r.sendCommand("QUIT")
	return err
}

func (r *Redis) RandomKey() ([]byte, error) {
	rp, err := r.sendCommand("RANDOMKEY")
	if err != nil {
		return nil, err
	}
	return rp.Bulk, nil
}

func (r *Redis) Rename(key, newkey string) error {
	rp, err := r.sendCommand("RENAME", key, newkey)
	if err != nil {
		return err
	}
	return r.okStatusReturnValue(rp)
}

func (r *Redis) Renamenx(key, newkey string) (bool, error) {
	rp, err := r.sendCommand("RENAMENX", key, newkey)
	if err != nil {
		return false, err
	}
	return r.booleanReturnValue(rp), nil
}

func (r *Redis) Restore(key string, ttl int, serialized []byte) error {
	rp, err := r.sendCommand("RESTORE", key, ttl, string(serialized))
	if err != nil {
		return err
	}
	return r.okStatusReturnValue(rp)
}

func (r *Redis) RPop(key string) ([]byte, error) {
	rp, err := r.sendCommand("RPOP", key)
	if err != nil {
		return nil, err
	}
	return rp.Bulk, nil
}

func (r *Redis) RPopLPush(source, destination string) ([]byte, error) {
	rp, err := r.sendCommand("RPOPLPUSH", source, destination)
	if err != nil {
		return nil, err
	}
	return rp.Bulk, nil
}

func (r *Redis) RPush(key string, values ...string) (int64, error) {
	args := r.packArgs("RPUSH", key, values)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp), nil
}

func (r *Redis) RPushx(key, value string) (int64, error) {
	rp, err := r.sendCommand("RPUSHX", key, value)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp), nil
}

func (r *Redis) SAdd(key string, members ...string) (int64, error) {
	args := r.packArgs("SADD", key, members)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp), nil
}

func (r *Redis) Save() error {
	rp, err := r.sendCommand("SAVE")
	if err != nil {
		return err
	}
	return r.okStatusReturnValue(rp)
}

func (r *Redis) SCard(key string) (int64, error) {
	rp, err := r.sendCommand("SCARD", key)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp), nil
}

func (r *Redis) ScriptExists(scripts ...string) ([]bool, error) {
	args := r.packArgs("SCRIPT", "EXISTS", scripts)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return nil, err
	}
	var result []bool
	for _, item := range rp.Multi {
		n, err := strconv.Atoi(string(item))
		if err != nil {
			return nil, err
		}
		result = append(result, n != 0)
	}
	return result, nil
}

func (r *Redis) ScriptFlush() error {
	rp, err := r.sendCommand("SCRIPT", "FLUSH")
	if err != nil {
		return err
	}
	return r.okStatusReturnValue(rp)
}

func (r *Redis) ScriptKill() error {
	rp, err := r.sendCommand("SCRIPT", "KILL")
	if err != nil {
		return err
	}
	return r.okStatusReturnValue(rp)
}

func (r *Redis) ScriptLoad(script string) (string, error) {
	rp, err := r.sendCommand("SCRIPT", "LOAD", script)
	if err != nil {
		return "", err
	}
	return r.stringBulkReturnValue(rp), nil
}

func (r *Redis) SDiff(keys ...string) ([]string, error) {
	args := r.packArgs("SDIFF", keys)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return nil, err
	}
	return r.listReturnValue(rp), nil
}

func (r *Redis) Set(key, value string, seconds, milliseconds int, must_exists, must_not_exists bool) error {
	args := r.packArgs("SET", key, value)
	if seconds > 0 {
		args = append(args, "EX", seconds)
	}
	if milliseconds > 0 {
		args = append(args, "PX", milliseconds)
	}
	if must_exists {
		args = append(args, "XX")
	} else if must_not_exists {
		args = append(args, "NX")
	}
	rp, err := r.sendCommand(args...)
	if err != nil {
		return err
	}
	return r.okStatusReturnValue(rp)
}

func (r *Redis) SetBit(key string, offset, value int) (int64, error) {
	rp, err := r.sendCommand("SETBIT", key, offset, value)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp), nil
}

func (r *Redis) Setex(key string, seconds int, value string) error {
	rp, err := r.sendCommand("SETEX", key, seconds, value)
	if err != nil {
		return err
	}
	return r.okStatusReturnValue(rp)
}

func (r *Redis) SetRange(key string, offset int, value string) (int64, error) {
	rp, err := r.sendCommand("SETRANGE", key, offset, value)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp), nil
}

func (r *Redis) Shutdown(save, no_save bool) error {
	args := []interface{}{"SHUTDOWN"}
	if save {
		args = append(args, "SAVE")
	} else if no_save {
		args = append(args, "NOSAVE")
	}
	rp, err := r.sendCommand(args...)
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}
	return errors.New(rp.Status)
}

func (r *Redis) SInter(keys ...string) ([]string, error) {
	args := r.packArgs("SINTER", keys)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return nil, err
	}
	return r.listReturnValue(rp), nil
}

func (r *Redis) SInterStore(destination string, keys ...string) (int64, error) {
	args := r.packArgs("SINTERSTORE", destination, keys)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp), nil
}

func (r *Redis) SIsMember(key, member string) (bool, error) {
	rp, err := r.sendCommand("SISMEMBER", key, member)
	if err != nil {
		return false, err
	}
	return r.booleanReturnValue(rp), nil
}

func (r *Redis) SlaveOf(host, port string) error {
	rp, err := r.sendCommand("SLAVEOF", host, port)
	if err != nil {
		return err
	}
	return r.okStatusReturnValue(rp)
}

func (r *Redis) SMembers(key string) ([]string, error) {
	rp, err := r.sendCommand("SMEMBERS", key)
	if err != nil {
		return nil, err
	}
	return r.listReturnValue(rp), nil
}

func (r *Redis) SMove(source, destination, member string) (bool, error) {
	rp, err := r.sendCommand("SMOVE", source, destination, member)
	if err != nil {
		return false, err
	}
	return r.booleanReturnValue(rp), nil
}

func (r *Redis) SPop(key string) ([]byte, error) {
	rp, err := r.sendCommand("SPOP", key)
	if err != nil {
		return nil, err
	}
	return rp.Bulk, nil
}

func (r *Redis) SRandMember(key string) ([]byte, error) {
	rp, err := r.sendCommand("SRANDMEMBER", key)
	if err != nil {
		return nil, err
	}
	return rp.Bulk, nil
}

func (r *Redis) SRandMemberCount(key string, count int) ([][]byte, error) {
	rp, err := r.sendCommand("SRANDMEMBER", key, count)
	if err != nil {
		return nil, err
	}
	return rp.Multi, nil
}

func (r *Redis) SRem(key string, members ...string) (int64, error) {
	args := r.packArgs("SREM", key, members)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp), nil
}

func (r *Redis) StrLen(key string) (int64, error) {
	rp, err := r.sendCommand("STRLEN", key)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp), nil
}

func (r *Redis) SUnion(keys ...string) ([]string, error) {
	args := r.packArgs("SUNION", keys)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return nil, err
	}
	return r.listReturnValue(rp), nil
}

func (r *Redis) SUnionStore(destination string, keys ...string) (int64, error) {
	args := r.packArgs("SUNIONSTORE", destination, keys)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp), nil
}

func (r *Redis) Time() (int, int, error) {
	rp, err := r.sendCommand("TIME")
	if err != nil {
		return 0, 0, err
	}
	seconds, err := strconv.Atoi(string(rp.Multi[0]))
	if err != nil {
		return 0, 0, err
	}
	microseconds, err := strconv.Atoi(string(rp.Multi[1]))
	if err != nil {
		return 0, 0, err
	}
	return seconds, microseconds, nil
}

func (r *Redis) TTL(key string) (int64, error) {
	rp, err := r.sendCommand("TTL", key)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp), nil
}

func (r *Redis) Type(key string) (string, error) {
	rp, err := r.sendCommand("TYPE", key)
	if err != nil {
		return "", err
	}
	return rp.Status, nil
}

func (r *Redis) ZAdd(key string, pairs map[float32]string) (int64, error) {
	args := r.packArgs("ZADD", key, pairs)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp), nil
}

func (r *Redis) ZCard(key string) (int64, error) {
	rp, err := r.sendCommand("ZCARD", key)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp), nil
}

func (r *Redis) ZCount(key string, min, max float32) (int64, error) {
	rp, err := r.sendCommand("ZCOUNT", key, min, max)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp), nil
}

func (r *Redis) ZIncrBy(key string, increment float32, member string) (float32, error) {
	rp, err := r.sendCommand("ZINCRBY", key, increment, member)
	if err != nil {
		return 0.0, err
	}
	score, err := strconv.ParseFloat(string(rp.Bulk), 32)
	if err != nil {
		return 0.0, err
	}
	return float32(score), nil
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
