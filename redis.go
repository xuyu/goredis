package redis

import (
	"bufio"
	"errors"
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

	DefaultDialTimeout time.Duration = 15 * time.Second
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

func DialTimeout(network, address string, timeout time.Duration) (*Redis, error) {
	r := &Redis{Network: network, Address: address, Timeout: timeout}
	conn, err := net.DialTimeout(r.Network, r.Address, r.Timeout)
	if err != nil {
		return nil, err
	}
	r.Conn = conn
	r.Reader = bufio.NewReader(r.Conn)
	r.Writer = bufio.NewWriter(r.Conn)
	return r, nil
}

func Dial(network, address string) (*Redis, error) {
	return DialTimeout(network, address, DefaultDialTimeout)
}

func (r *Redis) Close() error {
	if r.Conn != nil {
		return r.Conn.Close()
	}
	return nil
}

func (r *Redis) Send(args ...interface{}) error {
	if err := r.Writer.WriteByte(STAR); err != nil {
		return err
	}
	if _, err := r.Writer.WriteString(strconv.Itoa(len(args))); err != nil {
		return err
	}
	if _, err := r.Writer.Write(DELIM); err != nil {
		return err
	}
	for _, arg := range args {
		s := fmt.Sprint(arg)
		if err := r.Writer.WriteByte(DOLLAR); err != nil {
			return err
		}
		if _, err := r.Writer.WriteString(strconv.Itoa(len(s))); err != nil {
			return err
		}
		if _, err := r.Writer.Write(DELIM); err != nil {
			return err
		}
		if _, err := r.Writer.WriteString(s); err != nil {
			return err
		}
		if _, err := r.Writer.Write(DELIM); err != nil {
			return err
		}
	}
	if err := r.Writer.Flush(); err != nil {
		return err
	}
	return nil
}

func (r *Redis) SendRecv(args ...interface{}) (*Reply, error) {
	if err := r.Send(args...); err != nil {
		return nil, err
	}
	return r.Recv()
}

func OK(reply *Reply, err error) error {
	if err != nil {
		return err
	}
	if reply.Type != StatusReply {
		return errors.New("Make ok error")
	}
	if reply.Status == "OK" {
		return nil
	}
	return errors.New(reply.Status)
}

func Bool(reply *Reply, err error) (bool, error) {
	if err != nil {
		return false, err
	}
	if reply.Type != IntReply {
		return false, errors.New("Make bool error")
	}
	if reply.Int == 0 {
		return false, nil
	}
	return true, nil
}

func Map(reply *Reply, err error) (map[string][]byte, error) {
	if err != nil {
		return nil, err
	}
	if reply.Type != MultiReply || reply.Multi == nil {
		return nil, errors.New("Make map error")
	}
	result := make(map[string][]byte, len(reply.Multi)/2)
	for i := 0; i < len(reply.Multi)/2; i++ {
		key := reply.Multi[i*2]
		if key == nil {
			return nil, errors.New("Nil Bulk Error")
		}
		result[string(key)] = reply.Multi[i*2+1]
	}
	return result, nil
}
