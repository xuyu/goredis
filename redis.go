package redis

import (
	"bufio"
	"net"
	"time"
)

type Redis struct {
	network         string
	address         string
	database        int
	password        string
	socket_timeout  time.Duration
	connect_timeout time.Duration
	conn            net.Conn
	reader          bufio.Reader
}

func (r *Redis) Connect() error {
	c, err := net.DialTimeout(r.network, r.address, r.connect_timeout)
	if err != nil {
		return err
	}
	r.conn = c
	r.conn.SetDeadline(r.socket_timeout)
	r.reader = bufio.NewReader(r.conn)
	if r.password != "" {
		err := r.Auth(r.password)
		if err != nil {
			return err
		}
	}
	if r.database != 0 {
		err := r.Select(r.database)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Redis) send_command(args ...string) error {
	buf, err := build_request(args...)
	if err != nil {
		return err
	}
	_, er := r.conn.Write(buf)
	return er
}
