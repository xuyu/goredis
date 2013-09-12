package redis

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"time"
)

const (
	CR     byte = 13
	LF     byte = 10
	DOLLAR byte = 36
	COLON  byte = 58
	MINUS  byte = 45
	PLUS   byte = 43
	STAR   byte = 42
)

var (
	DELIM = []byte{CR, LF}
)

type Redis struct {
	Network string
	Address string
	Timeout time.Duration
	Conn    net.Conn
	Reader  *bufio.Reader
	Writer  *bufio.Writer
}

func NewClient(network, address string, timeout time.Duration) (*Redis, error) {
	r := &Redis{Network: network, Address: address, Timeout: timeout}
	err := r.Connect()
	return r, err
}

func (r *Redis) Connect() error {
	conn, err := net.DialTimeout(r.Network, r.Address, r.Timeout)
	if err != nil {
		return err
	}
	r.Conn = conn
	r.Reader = bufio.NewReader(r.Conn)
	r.Writer = bufio.NewWriter(r.Conn)
	return nil
}

func (r *Redis) Close() error {
	if r.Conn != nil {
		return r.Conn.Close()
	}
	return nil
}

func (r *Redis) Command(args ...interface{}) *Reply {
	rep := &Reply{}
	if err := r.Writer.WriteByte(STAR); err != nil {
		rep.Error = err
		return rep
	}
	if _, err := r.Writer.WriteString(strconv.Itoa(len(args))); err != nil {
		rep.Error = err
		return rep
	}
	if _, err := r.Writer.Write(DELIM); err != nil {
		rep.Error = err
		return rep
	}
	for _, arg := range args {
		s := fmt.Sprint(arg)
		if err := r.Writer.WriteByte(DOLLAR); err != nil {
			rep.Error = err
			return rep
		}
		if _, err := r.Writer.WriteString(strconv.Itoa(len(s))); err != nil {
			rep.Error = err
			return rep
		}
		if _, err := r.Writer.Write(DELIM); err != nil {
			rep.Error = err
			return rep
		}
		if _, err := r.Writer.WriteString(s); err != nil {
			rep.Error = err
			return rep
		}
		if _, err := r.Writer.Write(DELIM); err != nil {
			rep.Error = err
			return rep
		}
	}
	if err := r.Writer.Flush(); err != nil {
		rep.Error = err
		return rep
	}
	r.get_reply(rep)
	return rep
}
