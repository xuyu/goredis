package redis

import (
	"bufio"
	"errors"
	"strconv"
)

func read_reply(r bufio.Reader) (*Reply, error) {
	data, err := r.ReadBytes(LF)
	if err != nil {
		return nil, err
	}
	head := data[1 : len(data)-2]
	switch data[0] {
	case MINUS:
		return parse_error(r, head)
	case PLUS:
		return parse_status(r, head)
	case COLON:
		return parse_integer(r, head)
	case DOLLAR:
		return parse_bulk(r, head)
	case STAR:
		return parse_multibulk(r, head)
	}
	return nil, errors.New("Unknown Reply: " + string(data))
}

func parse_error(r bufio.Reader, head []byte) (*Reply, error) {
	return &Reply{Head: ERROR_REPLY, Error: errors.New(string(head))}, nil
}

func parse_status(r bufio.Reader, head []byte) (*Reply, error) {
	return &Reply{Head: STATUS_REPLY, Content: head}, nil
}

func parse_integer(r bufio.Reader, head []byte) (*Reply, error) {
	_, err := strconv.Atoi(string(head))
	if err != nil {
		return nil, err
	}
	return &Reply{Head: INTEGER_REPLY, Content: head}, nil
}

func parse_bulk(r bufio.Reader, head []byte) (*Reply, error) {
	reply := &Reply{Head: BULK_REPLY, Null: false}
	size, err := strconv.Atoi(string(head))
	if err != nil {
		return nil, err
	}
	if size == -1 {
		reply.Null = true
		return reply, nil
	}
	buf := make([]byte, size+2)
	if _, err := r.Read(buf); err != nil {
		return nil, err
	}
	reply.Content = buf[:len(buf)-2]
	return reply, nil
}

func parse_multibulk(r bufio.Reader, head []byte) (*Reply, error) {
	return nil, nil
}
