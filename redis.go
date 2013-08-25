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
	connect_timeout time.Duration
	conn            net.Conn
	reader          *bufio.Reader
}

func (r *Redis) Connect() error {
	if r.network == "" {
		r.network = "tcp"
	}
	if r.address == "" {
		r.address = ":6379"
	}
	var err error
	if r.connect_timeout > 0 {
		r.conn, err = net.DialTimeout(r.network, r.address, r.connect_timeout)
	} else {
		r.conn, err = net.Dial(r.network, r.address)
	}
	if err != nil {
		return err
	}
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
