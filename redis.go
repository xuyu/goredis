package redis

import (
	"bufio"
	"fmt"
	"net"
	"time"
)

type Redis struct {
	Network string
	Address string
	Timeout time.Duration
	Conn    net.Conn
	Reader  *bufio.Reader
}

func NewClient(network, address string, timeout time.Duration) *Redis {
	return &Redis{Network: network, Address: address, Timeout: timeout}
}

func (r *Redis) Connect() error {
	if conn, err := net.DialTimeout(r.Network, r.Address, r.Timeout); err != nil {
		return err
	} else {
		r.Conn = conn
		r.Reader = bufio.NewReader(r.Conn)
		return nil
	}
}

func (r *Redis) Close() error {
	if r.Conn != nil {
		return r.Conn.Close()
	}
	return nil
}

func (r *Redis) SendCommand(args ...interface{}) error {
	fields := [][]byte{}
	for _, arg := range args {
		fields = append(fields, []byte(fmt.Sprint(arg)))
	}
	if buf, err := BuildRequest(fields); err != nil {
		return err
	} else {
		_, err := r.Conn.Write(buf)
		return err
	}
}
