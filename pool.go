package goredis

import (
	"bufio"
	"errors"
	"net"
)

func (r *Redis) getConnection() (*Connection, error) {
	c := <-r.pool
	if c == nil {
		return r.openConnection()
	}
	return c, nil
}

func (r *Redis) activeConnection(c *Connection) {
	r.pool <- c
}

func (r *Redis) openConnection() (*Connection, error) {
	conn, err := net.DialTimeout(r.network, r.address, r.timeout)
	if err != nil {
		return nil, err
	}
	c := &Connection{conn, bufio.NewReader(conn)}
	if r.password != "" {
		if err := c.SendCommand("AUTH", r.password); err != nil {
			return nil, err
		}
		rp, err := c.RecvReply()
		if err != nil {
			return nil, err
		}
		if rp.Type == ErrorReply {
			return nil, errors.New(rp.Error)
		}
	}
	if r.db > 0 {
		if err := c.SendCommand("SELECT", r.db); err != nil {
			return nil, err
		}
		rp, err := c.RecvReply()
		if err != nil {
			return nil, err
		}
		if rp.Type == ErrorReply {
			return nil, errors.New(rp.Error)
		}
	}
	return c, nil
}
