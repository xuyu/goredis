// Redis Golang Client with full features
//
// Protocol Specification: http://redis.io/topics/protocol.
//
// Redis reply has five types: status, error, integer, bulk, multi bulk.
// A Status Reply is in the form of a single line string starting with "+" terminated by "\r\n".
// Error Replies are very similar to Status Replies. The only difference is that the first byte is "-".
// Integer reply is just a CRLF terminated string representing an integer, prefixed by a ":" byte.
// Bulk replies are used by the server in order to return a single binary safe string up to 512 MB in length.
// A Multi bulk reply is used to return an array of other replies.
// Every element of a Multi Bulk Reply can be of any kind, including a nested Multi Bulk Reply.
// So five reply type is defined:
//  const (
//  	ErrorReply = iota
//  	StatusReply
//  	IntegerReply
//  	BulkReply
//  	MultiReply
//  )
// And then a Reply struct which represent the redis response data is defined:
//  type Reply struct {
//  	Type    int
//  	Error   string
//  	Status  string
//  	Integer int64  // Support Redis 64bit integer
//  	Bulk    []byte // Support Redis Null Bulk Reply
//  	Multi   []*Reply
//  }
// Reply struct has many useful methods:
//  func (rp *Reply) IntegerValue() (int64, error)
//  func (rp *Reply) BoolValue() (bool, error)
//  func (rp *Reply) StatusValue() (string, error)
//  func (rp *Reply) OKValue() error
//  func (rp *Reply) BytesValue() ([]byte, error)
//  func (rp *Reply) StringValue() (string, error)
//  func (rp *Reply) MultiValue() ([]*Reply, error)
//  func (rp *Reply) HashValue() (map[string]string, error)
//  func (rp *Reply) ListValue() ([]string, error)
//  func (rp *Reply) BytesArrayValue() ([][]byte, error)
//  func (rp *Reply) BoolArrayValue() ([]bool, error)
//
// Connect redis has two function: DialTimeout and DialURL, for example:
//  client, err := DialTimeout("tcp", "127.0.0.1:6379", 0, "", 10*time.Second, 10)
//  client, err := DialURL("redis://auth:password@127.0.0.1:6379/0?timeout=10s&size=10")
//
// Try a redis command is simple too, let's do GET/SET:
//  err := client.Set("key", "value", 0, 0, false, false)
//  value, err := client.Get("key")
//
// Or you can execute customer command with Redis.ExecuteCommand method:
//  reply, err := client.ExecuteCommand("SET", "key", "value")
//  err := reply.OKValue()
//
// Redis Pipelining is defined as:
//  type Pipelined struct {
//  	redis *Redis
// 		conn  *Connection
//  	times int
//  }
//  func (p *Pipelined) Close()
//  func (p *Pipelined) Command(args ...interface{})
//  func (p *Pipelined) Receive() (*Reply, error)
//  func (p *Pipelined) ReceiveAll() ([]*Reply, error)
//
// Transaction, Lua Eval, Publish/Subscribe, Monitor, Scan, Sort are also supported.

package goredis

import (
	"io"
	"time"
)

var (
	// Max connections of one redis client connection pool
	MAX_CONNECTIONS = 1024
)

type Redis struct {
	network  string
	address  string
	db       int
	password string
	timeout  time.Duration
	size     int
	pool     chan *Connection
}

func (r *Redis) ExecuteCommand(args ...interface{}) (*Reply, error) {
	c, err := r.getConnection()
	defer r.activeConnection(c)
	if err != nil {
		return nil, err
	}
	if err := c.SendCommand(args...); err != nil {
		if err != io.EOF {
			return nil, err
		}
		c, err = r.openConnection()
		if err != nil {
			return nil, err
		}
		if err = c.SendCommand(args...); err != nil {
			return nil, err
		}
	}
	rp, err := c.RecvReply()
	if err != nil {
		if err != io.EOF {
			return nil, err
		}
		c, err = r.openConnection()
		if err != nil {
			return nil, err
		}
		if err = c.SendCommand(args...); err != nil {
			return nil, err
		}
		return c.RecvReply()
	}
	return rp, err
}
