// Redis Golang Client
// Protocol Specification: http://redis.io/topics/protocol
//
// Networking layer
// A client connects to a Redis server creating a TCP connection to the port 6379.
// Every Redis command or data transmitted by the client and the server is terminated by \r\n (CRLF).
//
// A Status Reply (or: single line reply) is in the form of a single line string starting with "+" terminated by "\r\n".
//
// Error Replies are very similar to Status Replies. The only difference is that the first byte is "-".
//
// Integer reply is just a CRLF terminated string representing an integer, prefixed by a ":" byte.
//
// Bulk replies are used by the server in order to return a single binary safe string up to 512 MB in length.
//
// A Multi bulk reply is used to return an array of other replies.
// Every element of a Multi Bulk Reply can be of any kind, including a nested Multi Bulk Reply.
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
	// Max connections of one redis client connection pool
	MAX_CONNECTIONS = 1024
)

// Reply Type: Status, Integer, Bulk, Multi Bulk
// Error Reply Type return error directly
const (
	ErrorReply = iota
	StatusReply
	IntegerReply
	BulkReply
	MultiReply
)

// Represent Redis Reply
type Reply struct {
	Type    int
	Error   string
	Status  string
	Integer int64  // Support Redis 64bit integer
	Bulk    []byte // Support Redis Null Bulk Reply
	Multi   []*Reply
}

func packArgs(items ...interface{}) (args []interface{}) {
	for _, item := range items {
		v := reflect.ValueOf(item)
		switch v.Kind() {
		case reflect.Slice:
			if v.IsNil() {
				continue
			}
			if v.Type().Elem().Kind() == reflect.Uint8 {
				args = append(args, string(v.Bytes()))
			} else {
				for i := 0; i < v.Len(); i++ {
					args = append(args, v.Index(i).Interface())
				}
			}
		case reflect.Map:
			if v.IsNil() {
				continue
			}
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

func packCommand(args ...interface{}) ([]byte, error) {
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

type connection struct {
	conn   net.Conn
	reader *bufio.Reader
}

func (c *connection) sendCommand(args ...interface{}) error {
	request, err := packCommand(args...)
	if err != nil {
		return err
	}
	if _, err := c.conn.Write(request); err != nil {
		return err
	}
	return nil
}

func (c *connection) recvReply() (*Reply, error) {
	line, err := c.reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	line = line[:len(line)-2]
	switch line[0] {
	case '-':
		return &Reply{
			Type:  ErrorReply,
			Error: string(line[1:]),
		}, nil
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
			Type:    IntegerReply,
			Integer: i,
		}, nil
	case '$':
		size, err := strconv.Atoi(string(line[1:]))
		if err != nil {
			return nil, err
		}
		bulk, err := c.readBulk(size)
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
		multi := make([]*Reply, i)
		for j := 0; j < i; j++ {
			rp, err := c.recvReply()
			if err != nil {
				return nil, err
			}
			multi[j] = rp
		}
		return &Reply{
			Type:  MultiReply,
			Multi: multi,
		}, nil
	}
	return nil, errors.New("redis protocol error")
}

func (c *connection) readBulk(size int) ([]byte, error) {
	// If the requested value does not exist the bulk reply will use the special value -1 as data length
	if size < 0 {
		return nil, nil
	}
	buf := make([]byte, size+2)
	if _, err := c.reader.Read(buf); err != nil {
		return nil, err
	}
	return buf[:size], nil
}

type Redis struct {
	network  string
	address  string
	db       int
	password string
	timeout  time.Duration
	size     int
	pool     chan *connection
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
		pool:     make(chan *connection, size),
	}
	for i := 0; i < size; i++ {
		r.pool <- nil
	}
	c, err := r.getConnection()
	if err != nil {
		return nil, err
	}
	r.activeConnection(c)
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
	password := ""
	path := strings.Trim(ul.Path, "/")
	if ul.User != nil {
		if pw, set := ul.User.Password(); set {
			password = pw
		}
	}
	db, err := strconv.Atoi(path)
	if err != nil {
		return nil, err
	}
	timeout, err := time.ParseDuration(ul.Query().Get("timeout"))
	if err != nil {
		return nil, err
	}
	size, err := strconv.Atoi(ul.Query().Get("size"))
	if err != nil {
		return nil, err
	}
	return DialTimeout(network, address, db, password, timeout, size)
}

func (r *Redis) getConnection() (*connection, error) {
	c := <-r.pool
	if c == nil {
		return r.openConnection()
	}
	return c, nil
}

func (r *Redis) activeConnection(c *connection) {
	r.pool <- c
}

func (r *Redis) openConnection() (*connection, error) {
	conn, err := net.DialTimeout(r.network, r.address, r.timeout)
	if err != nil {
		return nil, err
	}
	c := &connection{conn, bufio.NewReader(conn)}
	if r.password != "" {
		if err := c.sendCommand("AUTH", r.password); err != nil {
			return nil, err
		}
		if _, err := c.recvReply(); err != nil {
			return nil, err
		}
	}
	if r.db > 0 {
		if err := c.sendCommand("SELECT", r.db); err != nil {
			return nil, err
		}
		if _, err := c.recvReply(); err != nil {
			return nil, err
		}
	}
	return c, nil
}

func (r *Redis) sendCommand(args ...interface{}) (*Reply, error) {
	c, err := r.getConnection()
	defer r.activeConnection(c)
	if err != nil {
		return nil, err
	}
	if err := c.sendCommand(args...); err != nil {
		if err == io.EOF {
			c, err = r.openConnection()
			if err != nil {
				return nil, err
			}
		}
		return nil, err
	}
	return c.recvReply()
}

func (r *Redis) integerReturnValue(rp *Reply) (int64, error) {
	if rp.Type == ErrorReply {
		return 0, errors.New(rp.Error)
	}
	if rp.Type != IntegerReply {
		return 0, errors.New("invalid reply type, not integer")
	}
	return rp.Integer, nil
}

// Integer replies are also extensively used in order to return true or false.
// For instance commands like EXISTS or SISMEMBER will return 1 for true and 0 for false.
func (r *Redis) booleanReturnValue(rp *Reply) (bool, error) {
	if rp.Type == ErrorReply {
		return false, errors.New(rp.Error)
	}
	if rp.Type != IntegerReply {
		return false, errors.New("invalid reply type, not integer")
	}
	return rp.Integer != 0, nil
}

func (r *Redis) statusReturnValue(rp *Reply) (string, error) {
	if rp.Type == ErrorReply {
		return "", errors.New(rp.Error)
	}
	if rp.Type != StatusReply {
		return "", errors.New("invalid reply type, not status")
	}
	return rp.Status, nil
}

func (r *Redis) okStatusReturnValue(rp *Reply) error {
	if rp.Type == ErrorReply {
		return errors.New(rp.Error)
	}
	if rp.Type != StatusReply {
		return errors.New("invalid reply type, not status")
	}
	if rp.Status == "OK" {
		return nil
	}
	return errors.New(rp.Status)
}

func (r *Redis) bytesBulkReturnValue(rp *Reply) ([]byte, error) {
	if rp.Type == ErrorReply {
		return nil, errors.New(rp.Error)
	}
	if rp.Type != BulkReply {
		return nil, errors.New("invalid reply type, not bulk")
	}
	return rp.Bulk, nil
}

func (r *Redis) stringBulkReturnValue(rp *Reply) (string, error) {
	if rp.Type == ErrorReply {
		return "", errors.New(rp.Error)
	}
	if rp.Type != BulkReply {
		return "", errors.New("invalid reply type, not bulk")
	}
	if rp.Bulk == nil {
		return "", nil
	}
	return string(rp.Bulk), nil
}

func (r *Redis) multiReplyReturnValue(rp *Reply) ([]*Reply, error) {
	if rp.Type == ErrorReply {
		return nil, errors.New(rp.Error)
	}
	if rp.Type != MultiReply {
		return nil, errors.New("invalid reply type, not multi bulk")
	}
	return rp.Multi, nil
}

func (r *Redis) hashReturnValue(rp *Reply) (map[string]string, error) {
	if rp.Type == ErrorReply {
		return nil, errors.New(rp.Error)
	}
	if rp.Type != MultiReply {
		return nil, errors.New("invalid reply type, not multi bulk")
	}
	result := make(map[string]string)
	if rp.Multi != nil {
		length := len(rp.Multi)
		for i := 0; i < length/2; i++ {
			key, err := r.stringBulkReturnValue(rp.Multi[i*2])
			if err != nil {
				return nil, err
			}
			value, err := r.stringBulkReturnValue(rp.Multi[i*2+1])
			if err != nil {
				return nil, err
			}
			result[key] = value
		}
	}
	return result, nil
}

func (r *Redis) listReturnValue(rp *Reply) ([]string, error) {
	if rp.Type == ErrorReply {
		return nil, errors.New(rp.Error)
	}
	if rp.Type != MultiReply {
		return nil, errors.New("invalid reply type, not multi bulk")
	}
	var result []string
	if rp.Multi != nil {
		for _, subrp := range rp.Multi {
			item, err := r.stringBulkReturnValue(subrp)
			if err != nil {
				return nil, err
			}
			result = append(result, item)
		}
	}
	return result, nil
}

func (r *Redis) bytesArrayReturnValue(rp *Reply) ([][]byte, error) {
	if rp.Type == ErrorReply {
		return nil, errors.New(rp.Error)
	}
	if rp.Type != MultiReply {
		return nil, errors.New("invalid reply type, not multi bulk")
	}
	var result [][]byte
	if rp.Multi != nil {
		for _, subrp := range rp.Multi {
			b, err := r.bytesBulkReturnValue(subrp)
			if err != nil {
				return nil, err
			}
			result = append(result, b)
		}
	}
	return result, nil
}

func (r *Redis) boolArrayReturnValue(rp *Reply) ([]bool, error) {
	if rp.Type == ErrorReply {
		return nil, errors.New(rp.Error)
	}
	if rp.Type != MultiReply {
		return nil, errors.New("invalid reply type, not multi bulk")
	}
	var result []bool
	if rp.Multi != nil {
		for _, subrp := range rp.Multi {
			b, err := r.booleanReturnValue(subrp)
			if err != nil {
				return nil, err
			}
			result = append(result, b)
		}
	}
	return result, nil
}

// Integer reply: the length of the string after the append operation.
func (r *Redis) Append(key, value string) (int64, error) {
	rp, err := r.sendCommand("APPEND", key, value)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

// If password matches the password in the configuration file,
// the server replies with the OK status code and starts accepting commands.
// Otherwise, an error is returned and the clients needs to try a new password.
func (r *Redis) Auth(password string) error {
	rp, err := r.sendCommand("AUTH", password)
	if err != nil {
		return err
	}
	return r.okStatusReturnValue(rp)
}

// Instruct Redis to start an Append Only File rewrite process.
// The rewrite will create a small optimized version of the current Append Only File.
func (r *Redis) BgRewriteAof() error {
	_, err := r.sendCommand("BGREWRITEAOF")
	return err
}

// Save the DB in background.
// The OK code is immediately returned.
// Redis forks, the parent continues to serve the clients, the child saves the DB on disk then exits.
// A client my be able to check if the operation succeeded using the LASTSAVE command.
func (r *Redis) BgSave() error {
	_, err := r.sendCommand("BGSAVE")
	return err
}

// Count the number of set bits (population counting) in a string.
func (r *Redis) BitCount(key, start, end string) (int64, error) {
	rp, err := r.sendCommand("BITCOUNT", key, start, end)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

// Perform a bitwise operation between multiple keys (containing string values) and store the result in the destination key.
// The BITOP command supports four bitwise operations: AND, OR, XOR and NOT, thus the valid forms to call the command are:
// BITOP AND destkey srckey1 srckey2 srckey3 ... srckeyN
// BITOP OR destkey srckey1 srckey2 srckey3 ... srckeyN
// BITOP XOR destkey srckey1 srckey2 srckey3 ... srckeyN
// BITOP NOT destkey srckey
// Return value: Integer reply
// The size of the string stored in the destination key, that is equal to the size of the longest input string.
func (r *Redis) BitOp(operation, destkey string, keys ...string) (int64, error) {
	args := packArgs("BITOP", operation, destkey, keys)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

// BLPOP is a blocking list pop primitive.
// It is the blocking version of LPOP
// because it blocks the connection when there are no elements to pop from any of the given lists.
// An element is popped from the head of the first list that is non-empty,
// with the given keys being checked in the order that they are given.
// A nil multi-bulk when no element could be popped and the timeout expired.
// A two-element multi-bulk with the first element being the name of the key where an element was popped
// and the second element being the value of the popped element.
func (r *Redis) BLPop(keys []string, timeout int) ([]string, error) {
	args := packArgs("BLPOP", keys, timeout)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return nil, err
	}
	if rp.Multi == nil {
		return nil, nil
	}
	return r.listReturnValue(rp)
}

// See the BLPOP documentation for the exact semantics,
// since BRPOP is identical to BLPOP with the only difference being that
// it pops elements from the tail of a list instead of popping from the head.
func (r *Redis) BRPop(keys []string, timeout int) ([]string, error) {
	args := packArgs("BRPOP", keys, timeout)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return nil, err
	}
	if rp.Multi == nil {
		return nil, nil
	}
	return r.listReturnValue(rp)
}

// BRPOPLPUSH is the blocking variant of RPOPLPUSH.
// When source contains elements,
// this command behaves exactly like RPOPLPUSH.
// When source is empty, Redis will block the connection until another client pushes to it or until timeout is reached.
// A timeout of zero can be used to block indefinitely.
// Bulk reply: the element being popped from source and pushed to destination.
// If timeout is reached, a Null multi-bulk reply is returned.
func (r *Redis) BRPopLPush(source, destination string, timeout int) ([]byte, error) {
	rp, err := r.sendCommand("BRPOPLPUSH", source, destination, timeout)
	if err != nil {
		return nil, err
	}
	if rp.Type == MultiReply {
		return nil, nil
	}
	return r.bytesBulkReturnValue(rp)
}

// The CLIENT KILL command closes a given client connection identified by ip:port.
// Due to the single-treaded nature of Redis, it is not possible to kill a client connection while it is executing a command.
// However, the client will notice the connection has been closed only when the next command is sent (and results in network error).
// Status code reply: OK if the connection exists and has been closed
func (r *Redis) ClientKill(ip string, port int) error {
	rp, err := r.sendCommand("CLIENT", "KILL", net.JoinHostPort(ip, strconv.Itoa(port)))
	if err != nil {
		return err
	}
	return r.okStatusReturnValue(rp)
}

// The CLIENT LIST command returns information and statistics about the client connections server in a mostly human readable format.
// Bulk reply: a unique string, formatted as follows:
// One client connection per line (separated by LF)
// Each line is composed of a succession of property=value fields separated by a space character.
func (r *Redis) ClientList() (string, error) {
	rp, err := r.sendCommand("CLIENT", "LIST")
	if err != nil {
		return "", err
	}
	return r.stringBulkReturnValue(rp)
}

// The CLIENT GETNAME returns the name of the current connection as set by CLIENT SETNAME.
// Since every new connection starts without an associated name,
// if no name was assigned a null bulk reply is returned.
func (r *Redis) ClientGetName() ([]byte, error) {
	rp, err := r.sendCommand("CLIENT", "GETNAME")
	if err != nil {
		return nil, err
	}
	return r.bytesBulkReturnValue(rp)
}

// The CLIENT SETNAME command assigns a name to the current connection.
func (r *Redis) ClientSetName(name string) error {
	rp, err := r.sendCommand("CLIENT", "SETNAME", name)
	if err != nil {
		return err
	}
	return r.okStatusReturnValue(rp)
}

// The CONFIG GET command is used to read the configuration parameters of a running Redis server.
// Not all the configuration parameters are supported in Redis 2.4,
// while Redis 2.6 can read the whole configuration of a server using this command.
// CONFIG GET takes a single argument, which is a glob-style pattern.
func (r *Redis) ConfigGet(parameter string) (map[string]string, error) {
	rp, err := r.sendCommand("CONFIG", "GET", parameter)
	if err != nil {
		return nil, err
	}
	return r.hashReturnValue(rp)
}

// The CONFIG REWRITE command rewrites the redis.conf file the server was started with,
// applying the minimal changes needed to make it reflecting the configuration currently used by the server,
// that may be different compared to the original one because of the use of the CONFIG SET command.
// Available since 2.8.0.
func (r *Redis) ConfigRewrite() error {
	rp, err := r.sendCommand("CONFIG", "REWRITE")
	if err != nil {
		return err
	}
	return r.okStatusReturnValue(rp)
}

// The CONFIG SET command is used in order to reconfigure the server at run time without the need to restart Redis.
//  You can change both trivial parameters or switch from one to another persistence option using this command.
func (r *Redis) ConfigSet(parameter, value string) error {
	rp, err := r.sendCommand("CONFIG", "SET")
	if err != nil {
		return err
	}
	return r.okStatusReturnValue(rp)
}

// Resets the statistics reported by Redis using the INFO command.
// These are the counters that are reset:
// Keyspace hits
// Keyspace misses
// Number of commands processed
// Number of connections received
// Number of expired keys
// Number of rejected connections
// Latest fork(2) time
// The aof_delayed_fsync counter
func (r *Redis) ConfigResetStat() error {
	_, err := r.sendCommand("CONFIG", "RESETSTAT")
	return err
}

// Return the number of keys in the currently-selected database.
func (r *Redis) DBSize() (int64, error) {
	rp, err := r.sendCommand("DBSIZE")
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

/*
DEBUG OBJECT key
DEBUG OBJECT is a debugging command that should not be used by clients.
*/

/*
DEBUG SEGFAULT
DEBUG SEGFAULT performs an invalid memory access that crashes Redis.
It is used to simulate bugs during the development.
*/

// Decrements the number stored at key by one.
// If the key does not exist, it is set to 0 before performing the operation.
// An error is returned if the key contains a value of the wrong type
// or contains a string that can not be represented as integer.
// This operation is limited to 64 bit signed integers.
// Integer reply: the value of key after the decrement
func (r *Redis) Decr(key string) (int64, error) {
	rp, err := r.sendCommand("DECR", key)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

// Decrements the number stored at key by decrement.
func (r *Redis) DecrBy(key string, decrement int) (int64, error) {
	rp, err := r.sendCommand("DECRBY", key, decrement)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

// Removes the specified keys.
// A key is ignored if it does not exist.
// Integer reply: The number of keys that were removed.
func (r *Redis) Del(keys ...string) (int64, error) {
	args := packArgs("DEL", keys)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

// Serialize the value stored at key in a Redis-specific format and return it to the user.
// The returned value can be synthesized back into a Redis key using the RESTORE command.
// Return []byte for maybe big data
func (r *Redis) Dump(key string) ([]byte, error) {
	rp, err := r.sendCommand("DUMP", key)
	if err != nil {
		return nil, err
	}
	return r.bytesBulkReturnValue(rp)
}

func (r *Redis) Echo(message string) (string, error) {
	rp, err := r.sendCommand("ECHO", message)
	if err != nil {
		return "", err
	}
	return r.stringBulkReturnValue(rp)
}

func (r *Redis) Eval(script string, keys []string, args []string) (*Reply, error) {
	cmds := packArgs("EVAL", script, len(keys), keys, args)
	return r.sendCommand(cmds...)
}

func (r *Redis) EvalSha(sha1 string, keys []string, args []string) (*Reply, error) {
	cmds := packArgs("EVALSHA", sha1, len(keys), keys, args)
	return r.sendCommand(cmds...)
}

func (r *Redis) Exists(key string) (bool, error) {
	rp, err := r.sendCommand("EXISTS", key)
	if err != nil {
		return false, err
	}
	return r.booleanReturnValue(rp)
}

// Set a timeout on key.
// After the timeout has expired, the key will automatically be deleted.
// A key with an associated timeout is often said to be volatile in Redis terminology.
func (r *Redis) Expire(key string, seconds int) (bool, error) {
	rp, err := r.sendCommand("EXPIRE", key, seconds)
	if err != nil {
		return false, err
	}
	return r.booleanReturnValue(rp)
}

// EXPIREAT has the same effect and semantic as EXPIRE,
// but instead of specifying the number of seconds representing the TTL (time to live),
// it takes an absolute Unix timestamp (seconds since January 1, 1970).
func (r *Redis) ExpireAt(key string, timestamp int64) (bool, error) {
	rp, err := r.sendCommand("EXPIREAT", key, timestamp)
	if err != nil {
		return false, err
	}
	return r.booleanReturnValue(rp)
}

// Delete all the keys of all the existing databases,
// not just the currently selected one.
// This command never fails.
func (r *Redis) FlushAll() error {
	_, err := r.sendCommand("FLUSHALL")
	return err
}

// Delete all the keys of the currently selected DB.
// This command never fails.
func (r *Redis) FlushDB() error {
	_, err := r.sendCommand("FLUSHDB")
	return err
}

// Get the value of key.
// If the key does not exist the special value nil is returned.
// An error is returned if the value stored at key is not a string,
// because GET only handles string values.
func (r *Redis) Get(key string) ([]byte, error) {
	rp, err := r.sendCommand("GET", key)
	if err != nil {
		return nil, err
	}
	return r.bytesBulkReturnValue(rp)
}

// Returns the bit value at offset in the string value stored at key.
// When offset is beyond the string length,
// the string is assumed to be a contiguous space with 0 bits.
// When key does not exist it is assumed to be an empty string,
// so offset is always out of range and the value is also assumed to be a contiguous space with 0 bits.
func (r *Redis) GetBit(key string, offset int) (int64, error) {
	rp, err := r.sendCommand("GETBIT", key, offset)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

// Returns the substring of the string value stored at key,
// determined by the offsets start and end (both are inclusive).
// Negative offsets can be used in order to provide an offset starting from the end of the string.
// So -1 means the last character, -2 the penultimate and so forth.
// The function handles out of range requests by limiting the resulting range to the actual length of the string.
func (r *Redis) GetRange(key string, start, end int) (string, error) {
	rp, err := r.sendCommand("GETRANGE", start, end)
	if err != nil {
		return "", err
	}
	return r.stringBulkReturnValue(rp)
}

// Atomically sets key to value and returns the old value stored at key.
// Returns an error when key exists but does not hold a string value.
func (r *Redis) GetSet(key, value string) (string, error) {
	rp, err := r.sendCommand("GETSET", key, value)
	if err != nil {
		return "", err
	}
	return r.stringBulkReturnValue(rp)
}

// Removes the specified fields from the hash stored at key.
// Specified fields that do not exist within this hash are ignored.
// If key does not exist, it is treated as an empty hash and this command returns 0.
func (r *Redis) HDel(key string, fields ...string) (int64, error) {
	args := packArgs("HDEL", key, fields)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

// Returns if field is an existing field in the hash stored at key.
func (r *Redis) HExists(key, field string) (bool, error) {
	rp, err := r.sendCommand("HEXISTS", key, field)
	if err != nil {
		return false, err
	}
	return r.booleanReturnValue(rp)
}

// Returns the value associated with field in the hash stored at key.
// Bulk reply: the value associated with field,
// or nil when field is not present in the hash or key does not exist.
func (r *Redis) HGet(key, field string) ([]byte, error) {
	rp, err := r.sendCommand("HGET", key, field)
	if err != nil {
		return nil, err
	}
	return r.bytesBulkReturnValue(rp)
}

// Returns all fields and values of the hash stored at key.
// In the returned value, every field name is followed by its value,
// so the length of the reply is twice the size of the hash.
func (r *Redis) HGetAll(key string) (map[string]string, error) {
	rp, err := r.sendCommand("HGETALL", key)
	if err != nil {
		return nil, err
	}
	return r.hashReturnValue(rp)
}

// Increments the number stored at field in the hash stored at key by increment.
// If key does not exist, a new key holding a hash is created.
// If field does not exist the value is set to 0 before the operation is performed.
// Integer reply: the value at field after the increment operation.
func (r *Redis) HIncrBy(key, field string, increment int) (int64, error) {
	rp, err := r.sendCommand("HINCRBY", key, field, increment)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

// Increment the specified field of an hash stored at key,
// and representing a floating point number, by the specified increment.
// If the field does not exist, it is set to 0 before performing the operation.
// An error is returned if one of the following conditions occur:
// The field contains a value of the wrong type (not a string).
// The current field content or the specified increment are not parsable as a double precision floating point number.
// Bulk reply: the value of field after the increment.
func (r *Redis) HIncrByFloat(key, field string, increment float64) (float64, error) {
	rp, err := r.sendCommand("HINCRBYFLOAT", key, field, increment)
	if err != nil {
		return 0.0, err
	}
	s, err := r.stringBulkReturnValue(rp)
	if err != nil {
		return 0.0, err
	}
	return strconv.ParseFloat(s, 64)
}

// eturns all field names in the hash stored at key.
// Multi-bulk reply: list of fields in the hash, or an empty list when key does not exist.
func (r *Redis) HKeys(key string) ([]string, error) {
	rp, err := r.sendCommand("HKEYS", key)
	if err != nil {
		return nil, err
	}
	return r.listReturnValue(rp)
}

// Returns the number of fields contained in the hash stored at key.
// Integer reply: number of fields in the hash, or 0 when key does not exist.
func (r *Redis) HLen(key string) (int64, error) {
	rp, err := r.sendCommand("HLEN", key)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

// Returns the values associated with the specified fields in the hash stored at key.
// For every field that does not exist in the hash, a nil value is returned.
// Because a non-existing keys are treated as empty hashes,
// running HMGET against a non-existing key will return a list of nil values.
// Multi-bulk reply: list of values associated with the given fields, in the same order as they are requested.
func (r *Redis) HMGet(key string, fields ...string) ([][]byte, error) {
	args := packArgs("HMGET", key, fields)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return nil, err
	}
	return r.bytesArrayReturnValue(rp)
}

// Sets the specified fields to their respective values in the hash stored at key.
// This command overwrites any existing fields in the hash.
// If key does not exist, a new key holding a hash is created.
func (r *Redis) HMSet(key string, pairs map[string]string) error {
	args := packArgs("HMSET", key, pairs)
	rp, err := r.sendCommand(args)
	if err != nil {
		return err
	}
	return r.okStatusReturnValue(rp)
}

// Sets field in the hash stored at key to value.
// If key does not exist, a new key holding a hash is created.
// If field already exists in the hash, it is overwritten.
func (r *Redis) HSet(key, field, value string) (bool, error) {
	rp, err := r.sendCommand("HSET", key, field, value)
	if err != nil {
		return false, err
	}
	return r.booleanReturnValue(rp)
}

// Sets field in the hash stored at key to value, only if field does not yet exist.
// If key does not exist, a new key holding a hash is created.
// If field already exists, this operation has no effect.
func (r *Redis) HSetnx(key, field, value string) (bool, error) {
	rp, err := r.sendCommand("HSETNX", key, field, value)
	if err != nil {
		return false, err
	}
	return r.booleanReturnValue(rp)
}

// Returns all values in the hash stored at key.
// Multi-bulk reply: list of values in the hash, or an empty list when key does not exist.
func (r *Redis) HVals(key string) ([]string, error) {
	rp, err := r.sendCommand("HVALS", key)
	if err != nil {
		return nil, err
	}
	return r.listReturnValue(rp)
}

// Increments the number stored at key by one.
// If the key does not exist, it is set to 0 before performing the operation.
// An error is returned if the key contains a value of the wrong type
// or contains a string that can not be represented as integer.
// Integer reply: the value of key after the increment
func (r *Redis) Incr(key string) (int64, error) {
	rp, err := r.sendCommand("INCR", key)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

// Increments the number stored at key by increment.
// If the key does not exist, it is set to 0 before performing the operation.
// An error is returned if the key contains a value of the wrong type
// or contains a string that can not be represented as integer.
// Integer reply: the value of key after the increment
func (r *Redis) IncrBy(key string, increment int) (int64, error) {
	rp, err := r.sendCommand("INCRBY", key, increment)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

// Bulk reply: the value of key after the increment.
func (r *Redis) IncrByFloat(key string, increment float64) (float64, error) {
	rp, err := r.sendCommand("INCRBYFLOAT", key, increment)
	if err != nil {
		return 0.0, err
	}
	s, err := r.stringBulkReturnValue(rp)
	if err != nil {
		return 0.0, err
	}
	return strconv.ParseFloat(s, 64)
}

// The INFO command returns information and statistics about the server
// in a format that is simple to parse by computers and easy to read by humans.
// format document at http://redis.io/commands/info
func (r *Redis) Info(section string) (string, error) {
	args := packArgs("INFO", section)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return "", err
	}
	return r.stringBulkReturnValue(rp)
}

// Returns all keys matching pattern.
func (r *Redis) Keys(pattern string) ([]string, error) {
	rp, err := r.sendCommand("KEYS", pattern)
	if err != nil {
		return nil, err
	}
	return r.listReturnValue(rp)
}

// Return the UNIX TIME of the last DB save executed with success.
// A client may check if a BGSAVE command succeeded reading the LASTSAVE value,
// then issuing a BGSAVE command and checking at regular intervals every N seconds if LASTSAVE changed.
// Integer reply: an UNIX time stamp.
func (r *Redis) LastSave() (int64, error) {
	rp, err := r.sendCommand("LASTSAVE")
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

// Returns the element at index index in the list stored at key.
// The index is zero-based, so 0 means the first element,
// 1 the second element and so on.
// Negative indices can be used to designate elements starting at the tail of the list.
// Here, -1 means the last element, -2 means the penultimate and so forth.
// When the value at key is not a list, an error is returned.
// Bulk reply: the requested element, or nil when index is out of range.
func (r *Redis) LIndex(key string, index int) ([]byte, error) {
	rp, err := r.sendCommand("LINDEX", key, index)
	if err != nil {
		return nil, err
	}
	return r.bytesBulkReturnValue(rp)
}

// Inserts value in the list stored at key either before or after the reference value pivot.
// When key does not exist, it is considered an empty list and no operation is performed.
// An error is returned when key exists but does not hold a list value.
// Integer reply: the length of the list after the insert operation, or -1 when the value pivot was not found.
func (r *Redis) LInsert(key, position, pivot, value string) (int64, error) {
	rp, err := r.sendCommand("LINSERT", key, position, pivot, value)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

// Returns the length of the list stored at key.
// If key does not exist, it is interpreted as an empty list and 0 is returned.
// An error is returned when the value stored at key is not a list.
func (r *Redis) LLen(key string) (int64, error) {
	rp, err := r.sendCommand("LLEN", key)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

// Removes and returns the first element of the list stored at key.
// Bulk reply: the value of the first element, or nil when key does not exist.
func (r *Redis) LPop(key string) ([]byte, error) {
	rp, err := r.sendCommand("LPOP", key)
	if err != nil {
		return nil, err
	}
	return r.bytesBulkReturnValue(rp)
}

// Insert all the specified values at the head of the list stored at key.
// If key does not exist, it is created as empty list before performing the push operations.
// When key holds a value that is not a list, an error is returned.
// Integer reply: the length of the list after the push operations.
func (r *Redis) LPush(key string, values ...string) (int64, error) {
	args := packArgs("LPUSH", key, values)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

// Inserts value at the head of the list stored at key,
// only if key already exists and holds a list.
// In contrary to LPUSH, no operation will be performed when key does not yet exist.
// Integer reply: the length of the list after the push operation.
func (r *Redis) LPushx(key, value string) (int64, error) {
	rp, err := r.sendCommand("LPUSHX", key, value)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

// Returns the specified elements of the list stored at key.
// The offsets start and stop are zero-based indexes,
// with 0 being the first element of the list (the head of the list), 1 being the next element and so on.
// These offsets can also be negative numbers indicating offsets starting at the end of the list.
// For example, -1 is the last element of the list, -2 the penultimate, and so on.
//
// Note that if you have a list of numbers from 0 to 100, LRANGE list 0 10 will return 11 elements,
// that is, the rightmost item is included.
// Out of range indexes will not produce an error.
// If start is larger than the end of the list, an empty list is returned.
// If stop is larger than the actual end of the list, Redis will treat it like the last element of the list.
// Multi-bulk reply: list of elements in the specified range.
func (r *Redis) LRange(key string, start, end int) ([]string, error) {
	rp, err := r.sendCommand("LRANGE", key, start, end)
	if err != nil {
		return nil, err
	}
	return r.listReturnValue(rp)
}

// Removes the first count occurrences of elements equal to value from the list stored at key.
// The count argument influences the operation in the following ways:
// count > 0: Remove elements equal to value moving from head to tail.
// count < 0: Remove elements equal to value moving from tail to head.
// count = 0: Remove all elements equal to value.
// Integer reply: the number of removed elements.
func (r *Redis) LRem(key string, count int, value string) (int64, error) {
	rp, err := r.sendCommand("LREM", key, count, value)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

// Sets the list element at index to value. For more information on the index argument, see LINDEX.
// An error is returned for out of range indexes.
func (r *Redis) LSet(key string, index int, value string) error {
	rp, err := r.sendCommand("LSET", key, index, value)
	if err != nil {
		return err
	}
	return r.okStatusReturnValue(rp)
}

// Trim an existing list so that it will contain only the specified range of elements specified.
// Both start and stop are zero-based indexes, where 0 is the first element of the list (the head), 1 the next element and so on.
func (r *Redis) LTrim(key string, start, stop int) error {
	rp, err := r.sendCommand("LTRIM", key, start, stop)
	if err != nil {
		return err
	}
	return r.okStatusReturnValue(rp)
}

// Returns the values of all specified keys.
// For every key that does not hold a string value or does not exist,
// the special value nil is returned. Because of this, the operation never fails.
// Multi-bulk reply: list of values at the specified keys.
func (r *Redis) MGet(keys ...string) ([][]byte, error) {
	args := packArgs("MGET", keys)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return nil, err
	}
	return r.bytesArrayReturnValue(rp)
}

// Move key from the currently selected database (see SELECT) to the specified destination database.
// When key already exists in the destination database,
// or it does not exist in the source database, it does nothing.
func (r *Redis) Move(key string, db int) (bool, error) {
	rp, err := r.sendCommand("MOVE", key, db)
	if err != nil {
		return false, err
	}
	return r.booleanReturnValue(rp)
}

// Sets the given keys to their respective values.
// MSET replaces existing values with new values, just as regular SET.
// See MSETNX if you don't want to overwrite existing values.
func (r *Redis) MSet(pairs map[string]string) error {
	args := packArgs("MSET", pairs)
	_, err := r.sendCommand(args...)
	return err
}

// Sets the given keys to their respective values.
// MSETNX will not perform any operation at all even if just a single key already exists.
// True if the all the keys were set.
// False if no key was set (at least one key already existed).
func (r *Redis) MSetnx(pairs map[string]string) (bool, error) {
	args := packArgs("MSETNX", pairs)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return false, err
	}
	return r.booleanReturnValue(rp)
}

/*
The OBJECT command allows to inspect the internals of Redis Objects associated with keys.
It is useful for debugging or to understand if your keys are using the specially encoded data types to save space.
Your application may also use the information reported by the OBJECT command to implement application level key eviction policies
when using Redis as a Cache.
*/

// Remove the existing timeout on key,
// turning the key from volatile (a key with an expire set) to persistent
// (a key that will never expire as no timeout is associated).
// True if the timeout was removed.
// False if key does not exist or does not have an associated timeout.
func (r *Redis) Persist(key string) (bool, error) {
	rp, err := r.sendCommand("PERSIST", key)
	if err != nil {
		return false, err
	}
	return r.booleanReturnValue(rp)
}

// This command works exactly like EXPIRE but the time to live of the key is specified in milliseconds instead of seconds.
func (r *Redis) PExpire(key string, milliseconds int) (bool, error) {
	rp, err := r.sendCommand("PEXPIRE", key, milliseconds)
	if err != nil {
		return false, err
	}
	return r.booleanReturnValue(rp)
}

// PEXPIREAT has the same effect and semantic as EXPIREAT,
// but the Unix time at which the key will expire is specified in milliseconds instead of seconds.
func (r *Redis) PExpireAt(key string, timestamp int64) (bool, error) {
	rp, err := r.sendCommand("PEXPIREAT", key, timestamp)
	if err != nil {
		return false, err
	}
	return r.booleanReturnValue(rp)
}

// Returns PONG. This command is often used to test if a connection is still alive, or to measure latency.
func (r *Redis) Ping() error {
	_, err := r.sendCommand("PING")
	return err
}

// PSETEX works exactly like SETEX with the sole difference that the expire time is specified in milliseconds instead of seconds.
func (r *Redis) PSetex(key string, milliseconds int, value string) error {
	_, err := r.sendCommand("PSETEX", key, milliseconds, value)
	return err
}

// Like TTL this command returns the remaining time to live of a key that has an expire set,
// with the sole difference that TTL returns the amount of remaining time in seconds while PTTL returns it in milliseconds.
func (r *Redis) PTTL(key string) (int64, error) {
	rp, err := r.sendCommand("PTTL", key)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

// Ask the server to close the connection.
// The connection is closed as soon as all pending replies have been written to the client.
func (r *Redis) Quit() error {
	_, err := r.sendCommand("QUIT")
	return err
}

// Return a random key from the currently selected database.
// Bulk reply: the random key, or nil when the database is empty.
func (r *Redis) RandomKey() ([]byte, error) {
	rp, err := r.sendCommand("RANDOMKEY")
	if err != nil {
		return nil, err
	}
	return r.bytesBulkReturnValue(rp)
}

// Renames key to newkey.
// It returns an error when the source and destination names are the same, or when key does not exist.
// If newkey already exists it is overwritten, when this happens RENAME executes an implicit DEL operation,
// so if the deleted key contains a very big value it may cause high latency even if RENAME itself is usually a constant-time operation.
func (r *Redis) Rename(key, newkey string) error {
	rp, err := r.sendCommand("RENAME", key, newkey)
	if err != nil {
		return err
	}
	return r.okStatusReturnValue(rp)
}

// Renames key to newkey if newkey does not yet exist.
// It returns an error under the same conditions as RENAME.
func (r *Redis) Renamenx(key, newkey string) (bool, error) {
	rp, err := r.sendCommand("RENAMENX", key, newkey)
	if err != nil {
		return false, err
	}
	return r.booleanReturnValue(rp)
}

// Create a key associated with a value that is obtained by deserializing the provided serialized value (obtained via DUMP).
// If ttl is 0 the key is created without any expire, otherwise the specified expire time (in milliseconds) is set.
// RESTORE checks the RDB version and data checksum. If they don't match an error is returned.
func (r *Redis) Restore(key string, ttl int, serialized string) error {
	rp, err := r.sendCommand("RESTORE", key, ttl, serialized)
	if err != nil {
		return err
	}
	return r.okStatusReturnValue(rp)
}

// Removes and returns the last element of the list stored at key.
// Bulk reply: the value of the last element, or nil when key does not exist.
func (r *Redis) RPop(key string) ([]byte, error) {
	rp, err := r.sendCommand("RPOP", key)
	if err != nil {
		return nil, err
	}
	return r.bytesBulkReturnValue(rp)
}

// Atomically returns and removes the last element (tail) of the list stored at source,
// and pushes the element at the first element (head) of the list stored at destination.
//
// If source does not exist, the value nil is returned and no operation is performed.
// If source and destination are the same,
// the operation is equivalent to removing the last element from the list and pushing it as first element of the list,
// so it can be considered as a list rotation command.
func (r *Redis) RPopLPush(source, destination string) ([]byte, error) {
	rp, err := r.sendCommand("RPOPLPUSH", source, destination)
	if err != nil {
		return nil, err
	}
	return r.bytesBulkReturnValue(rp)
}

// Insert all the specified values at the tail of the list stored at key.
// If key does not exist, it is created as empty list before performing the push operation.
// When key holds a value that is not a list, an error is returned.
func (r *Redis) RPush(key string, values ...string) (int64, error) {
	args := packArgs("RPUSH", key, values)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

// Inserts value at the tail of the list stored at key,
// only if key already exists and holds a list.
// In contrary to RPUSH, no operation will be performed when key does not yet exist.
func (r *Redis) RPushx(key, value string) (int64, error) {
	rp, err := r.sendCommand("RPUSHX", key, value)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

// Add the specified members to the set stored at key.
// Specified members that are already a member of this set are ignored.
// If key does not exist, a new set is created before adding the specified members.
// An error is returned when the value stored at key is not a set.
//
// Integer reply: the number of elements that were added to the set,
// not including all the elements already present into the set.
func (r *Redis) SAdd(key string, members ...string) (int64, error) {
	args := packArgs("SADD", key, members)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

// The SAVE commands performs a synchronous save of the dataset producing a point in time snapshot of all the data inside the Redis instance,
// in the form of an RDB file.
//
// You almost never want to call SAVE in production environments where it will block all the other clients. Instead usually BGSAVE is used.
func (r *Redis) Save() error {
	rp, err := r.sendCommand("SAVE")
	if err != nil {
		return err
	}
	return r.okStatusReturnValue(rp)
}

// Returns the set cardinality (number of elements) of the set stored at key.
func (r *Redis) SCard(key string) (int64, error) {
	rp, err := r.sendCommand("SCARD", key)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

// Returns information about the existence of the scripts in the script cache.
// Multi-bulk reply The command returns an array of integers that correspond to the specified SHA1 digest arguments.
// For every corresponding SHA1 digest of a script that actually exists in the script cache.
func (r *Redis) ScriptExists(scripts ...string) ([]bool, error) {
	args := packArgs("SCRIPT", "EXISTS", scripts)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return nil, err
	}
	return r.boolArrayReturnValue(rp)
}

// Flush the Lua scripts cache.
// Please refer to the EVAL documentation for detailed information about Redis Lua scripting.
func (r *Redis) ScriptFlush() error {
	rp, err := r.sendCommand("SCRIPT", "FLUSH")
	if err != nil {
		return err
	}
	return r.okStatusReturnValue(rp)
}

// Kills the currently executing Lua script, assuming no write operation was yet performed by the script.
func (r *Redis) ScriptKill() error {
	rp, err := r.sendCommand("SCRIPT", "KILL")
	if err != nil {
		return err
	}
	return r.okStatusReturnValue(rp)
}

// Load a script into the scripts cache, without executing it.
// After the specified command is loaded into the script cache it will be callable using EVALSHA with the correct SHA1 digest of the script,
// exactly like after the first successful invocation of EVAL.
// Bulk reply This command returns the SHA1 digest of the script added into the script cache.
func (r *Redis) ScriptLoad(script string) (string, error) {
	rp, err := r.sendCommand("SCRIPT", "LOAD", script)
	if err != nil {
		return "", err
	}
	return r.stringBulkReturnValue(rp)
}

// Returns the members of the set resulting from the difference between the first set and all the successive sets.
// Keys that do not exist are considered to be empty sets.
// Multi-bulk reply: list with members of the resulting set.
func (r *Redis) SDiff(keys ...string) ([]string, error) {
	args := packArgs("SDIFF", keys)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return nil, err
	}
	return r.listReturnValue(rp)
}

// Set key to hold the string value.
// If key already holds a value, it is overwritten, regardless of its type.
// Any previous time to live associated with the key is discarded on successful SET operation.
func (r *Redis) Set(key, value string, seconds, milliseconds int, must_exists, must_not_exists bool) error {
	args := packArgs("SET", key, value)
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

// Sets or clears the bit at offset in the string value stored at key.
// Integer reply: the original bit value stored at offset.
func (r *Redis) SetBit(key string, offset, value int) (int64, error) {
	rp, err := r.sendCommand("SETBIT", key, offset, value)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

// Set key to hold the string value and set key to timeout after a given number of seconds.
func (r *Redis) Setex(key string, seconds int, value string) error {
	rp, err := r.sendCommand("SETEX", key, seconds, value)
	if err != nil {
		return err
	}
	return r.okStatusReturnValue(rp)
}

// Set key to hold string value if key does not exist.
func (r *Redis) Setnx(key, value string) (bool, error) {
	rp, err := r.sendCommand("SETNX", key, value)
	if err != nil {
		return false, err
	}
	return r.booleanReturnValue(rp)
}

// Overwrites part of the string stored at key, starting at the specified offset, for the entire length of value.
// Integer reply: the length of the string after it was modified by the command.
func (r *Redis) SetRange(key string, offset int, value string) (int64, error) {
	rp, err := r.sendCommand("SETRANGE", key, offset, value)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

// The command behavior is the following:
// Stop all the clients.
// Perform a blocking SAVE if at least one save point is configured.
// Flush the Append Only File if AOF is enabled.
// Quit the server.
func (r *Redis) Shutdown(save, no_save bool) error {
	args := packArgs("SHUTDOWN")
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

// Returns the members of the set resulting from the intersection of all the given sets.
// Multi-bulk reply: list with members of the resulting set.
func (r *Redis) SInter(keys ...string) ([]string, error) {
	args := packArgs("SINTER", keys)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return nil, err
	}
	return r.listReturnValue(rp)
}

// This command is equal to SINTER, but instead of returning the resulting set, it is stored in destination.
// If destination already exists, it is overwritten.
// Integer reply: the number of elements in the resulting set.
func (r *Redis) SInterStore(destination string, keys ...string) (int64, error) {
	args := packArgs("SINTERSTORE", destination, keys)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

// Returns if member is a member of the set stored at key.
func (r *Redis) SIsMember(key, member string) (bool, error) {
	rp, err := r.sendCommand("SISMEMBER", key, member)
	if err != nil {
		return false, err
	}
	return r.booleanReturnValue(rp)
}

// The SLAVEOF command can change the replication settings of a slave on the fly.
// If a Redis server is already acting as slave, the command SLAVEOF NO ONE will turn off the replication,
// turning the Redis server into a MASTER.
// In the proper form SLAVEOF hostname port will make the server a slave of another server listening at the specified hostname and port.
//
// If a server is already a slave of some master,
// SLAVEOF hostname port will stop the replication against the old server and start the synchronization against the new one, discarding the old dataset.
// The form SLAVEOF NO ONE will stop replication, turning the server into a MASTER, but will not discard the replication.
// So, if the old master stops working, it is possible to turn the slave into a master and set the application to use this new master in read/write.
// Later when the other Redis server is fixed, it can be reconfigured to work as a slave.
func (r *Redis) SlaveOf(host, port string) error {
	rp, err := r.sendCommand("SLAVEOF", host, port)
	if err != nil {
		return err
	}
	return r.okStatusReturnValue(rp)
}

// Returns all the members of the set value stored at key.
func (r *Redis) SMembers(key string) ([]string, error) {
	rp, err := r.sendCommand("SMEMBERS", key)
	if err != nil {
		return nil, err
	}
	return r.listReturnValue(rp)
}

// Move member from the set at source to the set at destination. This operation is atomic.
// In every given moment the element will appear to be a member of source or destination for other clients.
func (r *Redis) SMove(source, destination, member string) (bool, error) {
	rp, err := r.sendCommand("SMOVE", source, destination, member)
	if err != nil {
		return false, err
	}
	return r.booleanReturnValue(rp)
}

/*
SORT key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]] [ASC|DESC] [ALPHA] [STORE destination]
*/

// Removes and returns a random element from the set value stored at key.
// Bulk reply: the removed element, or nil when key does not exist.
func (r *Redis) SPop(key string) ([]byte, error) {
	rp, err := r.sendCommand("SPOP", key)
	if err != nil {
		return nil, err
	}
	return r.bytesBulkReturnValue(rp)
}

// When called with just the key argument, return a random element from the set value stored at key.
// Bulk reply: the command returns a Bulk Reply with the randomly selected element, or nil when key does not exist.
func (r *Redis) SRandMember(key string) ([]byte, error) {
	rp, err := r.sendCommand("SRANDMEMBER", key)
	if err != nil {
		return nil, err
	}
	return r.bytesBulkReturnValue(rp)
}

// return an array of count distinct elements if count is positive.
// If called with a negative count the behavior changes and the command is allowed to return the same element multiple times.
// In this case the numer of returned elements is the absolute value of the specified count.
// returns an array of elements, or an empty array when key does not exist.
func (r *Redis) SRandMemberCount(key string, count int) ([][]byte, error) {
	rp, err := r.sendCommand("SRANDMEMBER", key, count)
	if err != nil {
		return nil, err
	}
	return r.bytesArrayReturnValue(rp)
}

// Remove the specified members from the set stored at key.
// Specified members that are not a member of this set are ignored.
// If key does not exist, it is treated as an empty set and this command returns 0.
// An error is returned when the value stored at key is not a set.
// Integer reply: the number of members that were removed from the set, not including non existing members.
func (r *Redis) SRem(key string, members ...string) (int64, error) {
	args := packArgs("SREM", key, members)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

// Returns the length of the string value stored at key.
// An error is returned when key holds a non-string value.
// Integer reply: the length of the string at key, or 0 when key does not exist.
func (r *Redis) StrLen(key string) (int64, error) {
	rp, err := r.sendCommand("STRLEN", key)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

// Returns the members of the set resulting from the union of all the given sets.
// Multi-bulk reply: list with members of the resulting set.
func (r *Redis) SUnion(keys ...string) ([]string, error) {
	args := packArgs("SUNION", keys)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return nil, err
	}
	return r.listReturnValue(rp)
}

// If destination already exists, it is overwritten.
// Integer reply: the number of elements in the resulting set.
func (r *Redis) SUnionStore(destination string, keys ...string) (int64, error) {
	args := packArgs("SUNIONSTORE", destination, keys)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

/*
SYNC
*/

// A multi bulk reply containing two elements:
// unix time in seconds.
// microseconds.
func (r *Redis) Time() ([]string, error) {
	rp, err := r.sendCommand("TIME")
	if err != nil {
		return nil, err
	}
	return r.listReturnValue(rp)
}

// Returns the remaining time to live of a key that has a timeout.
// Integer reply: TTL in seconds, or a negative value in order to signal an error (see the description above).
func (r *Redis) TTL(key string) (int64, error) {
	rp, err := r.sendCommand("TTL", key)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

// Returns the string representation of the type of the value stored at key.
// The different types that can be returned are: string, list, set, zset and hash.
// Status code reply: type of key, or none when key does not exist.
func (r *Redis) Type(key string) (string, error) {
	rp, err := r.sendCommand("TYPE", key)
	if err != nil {
		return "", err
	}
	return r.statusReturnValue(rp)
}

// Adds all the specified members with the specified scores to the sorted set stored at key.
// If a specified member is already a member of the sorted set,
// the score is updated and the element reinserted at the right position to ensure the correct ordering.
// If key does not exist, a new sorted set with the specified members as sole members is created,
// like if the sorted set was empty.
// If the key exists but does not hold a sorted set, an error is returned.
// Return value:
// The number of elements added to the sorted sets, not including elements already existing for which the score was updated.
func (r *Redis) ZAdd(key string, pairs map[float32]string) (int64, error) {
	args := packArgs("ZADD", key, pairs)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

// Returns the sorted set cardinality (number of elements) of the sorted set stored at key.
// Integer reply: the cardinality (number of elements) of the sorted set, or 0 if key does not exist.
func (r *Redis) ZCard(key string) (int64, error) {
	rp, err := r.sendCommand("ZCARD", key)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

// Returns the number of elements in the sorted set at key with a score between min and max.
// The min and max arguments have the same semantic as described for ZRANGEBYSCORE.
// Integer reply: the number of elements in the specified score range.
func (r *Redis) ZCount(key string, min, max float32) (int64, error) {
	rp, err := r.sendCommand("ZCOUNT", key, min, max)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

// Increments the score of member in the sorted set stored at key by increment.
// If member does not exist in the sorted set, it is added with increment as its score
// (as if its previous score was 0.0).
// If key does not exist, a new sorted set with the specified member as its sole member is created.
// An error is returned when key exists but does not hold a sorted set.
// Bulk reply: the new score of member (a double precision floating point number), represented as string.
func (r *Redis) ZIncrBy(key string, increment float32, member string) (float32, error) {
	rp, err := r.sendCommand("ZINCRBY", key, increment, member)
	if err != nil {
		return 0.0, err
	}
	s, err := r.stringBulkReturnValue(rp)
	if err != nil {
		return 0.0, err
	}
	score, err := strconv.ParseFloat(s, 32)
	if err != nil {
		return 0.0, err
	}
	return float32(score), nil
}

/*
ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
*/

/*
ZRANGE key start stop [WITHSCORES]
*/

/*
ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
*/

/*
ZRANK key member
*/

// Removes the specified members from the sorted set stored at key. Non existing members are ignored.
// An error is returned when key exists and does not hold a sorted set.
// Integer reply, specifically:
// The number of members removed from the sorted set, not including non existing members.
func (r *Redis) ZRem(key string, members ...string) (int64, error) {
	args := packArgs("ZREM", key, members)
	rp, err := r.sendCommand(args...)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

// Removes all elements in the sorted set stored at key with rank between start and stop.
// Both start and stop are 0 -based indexes with 0 being the element with the lowest score.
// These indexes can be negative numbers, where they indicate offsets starting at the element with the highest score.
// For example: -1 is the element with the highest score, -2 the element with the second highest score and so forth.
// Integer reply: the number of elements removed.
func (r *Redis) ZRemRangeByRank(key string, start, stop int) (int64, error) {
	rp, err := r.sendCommand("ZREMRANGEBYRANK", key, start, stop)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

// Removes all elements in the sorted set stored at key with a score between min and max (inclusive).
// Integer reply: the number of elements removed.
func (r *Redis) ZRemRangeByScore(key string, min, max int) (int64, error) {
	rp, err := r.sendCommand("ZREMRANGEBYSCORE", key, min, max)
	if err != nil {
		return 0, err
	}
	return r.integerReturnValue(rp)
}

/*
ZREVRANGE key start stop [WITHSCORES]
*/

// Returns the score of member in the sorted set at key.
// If member does not exist in the sorted set, or key does not exist, nil is returned.
// Bulk reply: the score of member (a double precision floating point number), represented as string.
func (r *Redis) ZScore(key, member string) ([]byte, error) {
	rp, err := r.sendCommand("ZSCORE", key, member)
	if err != nil {
		return nil, err
	}
	return r.bytesBulkReturnValue(rp)
}

/*
ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
*/

/*
SCAN cursor [MATCH pattern] [COUNT count]
SSCAN key cursor [MATCH pattern] [COUNT count]
SCAN key cursor [MATCH pattern] [COUNT count]
ZSCAN key cursor [MATCH pattern] [COUNT count]
*/

// Document: http://redis.io/topics/transactions
// MULTI, EXEC, DISCARD and WATCH are the foundation of transactions in Redis.
// A Redis script is transactional by definition,
// so everything you can do with a Redis transaction, you can also do with a script,
// and usually the script will be both simpler and faster.
func (r *Redis) Transaction() (*Transaction, error) {
	c, err := r.getConnection()
	if err != nil {
		return nil, err
	}
	return newTransaction(r, c)
}

type Transaction struct {
	redis *Redis
	c     *connection
}

func newTransaction(r *Redis, c *connection) (*Transaction, error) {
	t := &Transaction{
		redis: r,
		c:     c,
	}
	err := t.multi()
	if err != nil {
		r.activeConnection(c)
		return nil, err
	}
	return t, nil
}

func (t *Transaction) multi() error {
	if err := t.c.sendCommand("MULTI"); err != nil {
		return err
	}
	_, err := t.c.recvReply()
	return err
}

func (t *Transaction) Discard() error {
	if err := t.c.sendCommand("DISCARD"); err != nil {
		return err
	}
	_, err := t.c.recvReply()
	return err
}

func (t *Transaction) Watch(keys ...string) error {
	args := packArgs("WATCH", keys)
	if err := t.c.sendCommand(args...); err != nil {
		return err
	}
	_, err := t.c.recvReply()
	return err
}

func (t *Transaction) UnWatch() error {
	if err := t.c.sendCommand("UNWATCH"); err != nil {
		return err
	}
	_, err := t.c.recvReply()
	return err
}

func (t *Transaction) Exec() ([]*Reply, error) {
	if err := t.c.sendCommand("EXEC"); err != nil {
		return nil, err
	}
	rp, err := t.c.recvReply()
	if err != nil {
		return nil, err
	}
	return t.redis.multiReplyReturnValue(rp)
}

func (t *Transaction) Close() {
	t.redis.activeConnection(t.c)
}

func (t *Transaction) Command(args ...interface{}) error {
	args2 := packArgs(args...)
	if err := t.c.sendCommand(args2...); err != nil {
		return err
	}
	rp, err := t.c.recvReply()
	if err != nil {
		return err
	}
	s, err := t.redis.statusReturnValue(rp)
	if err != nil {
		return err
	}
	if s != "QUEUED" {
		return errors.New(s)
	}
	return nil
}
