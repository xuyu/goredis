package redis

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"
)

const (
	MAX_CONNECTIONS = 1024
)

const (
	StatusReply = iota
	NumberReply
	BulkReply
	MultiReply
)

type reply struct {
	Type   int
	Status string
	Number int64
	Bulk   []byte
	Multi  [][]byte
}

type Redis struct {
	network  string
	address  string
	db       int
	password string
	timeout  time.Duration
	size     int
	pool     chan net.Conn
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
		pool:     make(chan net.Conn, size),
	}
	for i := 0; i < size; i++ {
		r.pool <- nil
	}
	conn, err := r.getConnection()
	if err != nil {
		return nil, err
	}
	r.activeConnection(conn)
	return r, nil
}

func (r *Redis) getConnection() (net.Conn, error) {
	c := <-r.pool
	if c == nil {
		return r.openConnection()
	}
	return c, nil
}

func (r *Redis) activeConnection(conn net.Conn) {
	r.pool <- conn
}

func (r *Redis) openConnection() (net.Conn, error) {
	conn, err := net.DialTimeout(r.network, r.address, r.timeout)
	if err != nil {
		return nil, err
	}
	if r.password != "" {
		if err := r.sendConnectionCmd(conn, "AUTH", r.password); err != nil {
			return nil, err
		}
		if _, err := r.recvConnectionReply(conn); err != nil {
			return nil, err
		}
	}
	if r.db > 0 {
		if err := r.sendConnectionCmd(conn, "SELECT", r.db); err != nil {
			return nil, err
		}
		if _, err := r.recvConnectionReply(conn); err != nil {
			return nil, err
		}
	}
	return conn, nil
}

func (r *Redis) sendConnectionCmd(conn net.Conn, args ...interface{}) error {
	cmd, err := r.packCommand(args...)
	if err != nil {
		return err
	}
	if _, err := conn.Write(cmd); err != nil {
		return err
	}
	return nil
}

func (r *Redis) packCommand(args ...interface{}) ([]byte, error) {
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

func (r *Redis) recvConnectionReply(conn net.Conn) (*reply, error) {
	reader := bufio.NewReader(conn)
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	line = line[:len(line)-2]
	switch line[0] {
	case '-':
		return nil, errors.New(string(line[1:]))
	case '+':
		return &reply{
			Type:   StatusReply,
			Status: string(line[1:]),
		}, nil
	case ':':
		i, err := strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return nil, err
		}
		return &reply{
			Type:   NumberReply,
			Number: i,
		}, nil
	case '$':
		size, err := strconv.Atoi(string(line[1:]))
		if err != nil {
			return nil, err
		}
		bulk, err := r.readSize(reader, size)
		if err != nil {
			return nil, err
		}
		return &reply{
			Type: BulkReply,
			Bulk: bulk,
		}, nil
	case '*':
		i, err := strconv.Atoi(string(line[1:]))
		if err != nil {
			return nil, err
		}
		multi := make([][]byte, i)
		for j := 0; j < i; j++ {
			bulk, err := r.readBulk(reader)
			if err != nil {
				return nil, err
			}
			multi[j] = bulk
		}
		return &reply{
			Type:  MultiReply,
			Multi: multi,
		}, nil
	}
	return nil, errors.New("redis protocol error")
}

func (r *Redis) readBulk(reader *bufio.Reader) ([]byte, error) {
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if line[0] != '$' {
		return nil, errors.New("not bulk head prefix")
	}
	size, err := strconv.Atoi(string(line[1 : len(line)-2]))
	if err != nil {
		return nil, err
	}
	return r.readSize(reader, size)
}

func (r *Redis) readSize(reader *bufio.Reader, size int) ([]byte, error) {
	if size < 0 {
		return nil, nil
	}
	buf := make([]byte, size+2)
	if _, err := reader.Read(buf); err != nil {
		return nil, err
	}
	return buf[:size], nil
}

func (r *Redis) Close() {
	for i := 0; i < r.size; i++ {
		conn := <-r.pool
		if conn != nil {
			conn.Close()
		}
	}
}

func (r *Redis) sendCommand(args ...interface{}) (*reply, error) {
	conn, err := r.getConnection()
	defer r.activeConnection(conn)
	if err != nil {
		return nil, err
	}
	if err := r.sendConnectionCmd(conn, args...); err != nil {
		return nil, err
	}
	return r.recvConnectionReply(conn)
}
