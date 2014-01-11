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
