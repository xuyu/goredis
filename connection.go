package goredis

import (
	"bufio"
	"errors"
	"io"
	"net"
	"strconv"
)

type Connection struct {
	Conn   net.Conn
	Reader *bufio.Reader
}

func (c *Connection) SendCommand(args ...interface{}) error {
	request, err := packCommand(args...)
	if err != nil {
		return err
	}
	if _, err := c.Conn.Write(request); err != nil {
		return err
	}
	return nil
}

func (c *Connection) RecvReply() (*Reply, error) {
	line, err := c.Reader.ReadBytes('\n')
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
		rp := &Reply{Type: MultiReply}
		if i >= 0 {
			multi := make([]*Reply, i)
			for j := 0; j < i; j++ {
				rp, err := c.RecvReply()
				if err != nil {
					return nil, err
				}
				multi[j] = rp
			}
			rp.Multi = multi
		}
		return rp, nil
	}
	return nil, errors.New("redis protocol error")
}

func (c *Connection) readBulk(size int) ([]byte, error) {
	// If the requested value does not exist the bulk reply will use the special value -1 as data length
	if size < 0 {
		return nil, nil
	}
	buf := make([]byte, size+2)
	if _, err := io.ReadFull(c.Reader, buf); err != nil {
		return nil, err
	}
	return buf[:size], nil
}

// If password matches the password in the configuration file,
// the server replies with the OK status code and starts accepting commands.
// Otherwise, an error is returned and the clients needs to try a new password.
func (r *Redis) Auth(password string) error {
	rp, err := r.ExecuteCommand("AUTH", password)
	if err != nil {
		return err
	}
	return rp.OKValue()
}

// Returns message.
func (r *Redis) Echo(message string) (string, error) {
	rp, err := r.ExecuteCommand("ECHO", message)
	if err != nil {
		return "", err
	}
	return rp.StringValue()
}

// Returns PONG. This command is often used to test if a connection is still alive, or to measure latency.
func (r *Redis) Ping() error {
	_, err := r.ExecuteCommand("PING")
	return err
}

// QUIT
// Ask the server to close the connection.
// The connection is closed as soon as all pending replies have been written to the client.

// SELECT index
// Change the selected database for the current connection.
