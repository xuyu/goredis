package redis

import (
	"errors"
	"strconv"
)

const (
	ErrorReply = 1 << iota
	StatusReply
	IntReply
	BulkReply
	MultiReply
)

type Reply struct {
	Type   int
	Error  error
	Status string
	Int    int64
	Bulk   []byte
	Multi  [][]byte
}

func (r *Redis) Recv() (*Reply, error) {
	data, err := r.Reader.ReadBytes(LF)
	if err != nil {
		return nil, err
	}
	reply := &Reply{}
	head := data[1 : len(data)-2]
	switch data[0] {
	case MINUS:
		reply.Type = ErrorReply
		reply.Error = errors.New(string(data[1 : len(data)-2]))
	case PLUS:
		reply.Type = StatusReply
		reply.Status = string(head)
	case COLON:
		reply.Type = IntReply
		if i, err := strconv.ParseInt(string(head), 10, 64); err != nil {
			reply.Error = err
		} else {
			reply.Int = i
		}
	case DOLLAR:
		reply.Type = BulkReply
		if bulk, err := r.bulk_reply(head); err != nil {
			reply.Error = err
		} else {
			reply.Bulk = bulk
		}
	case STAR:
		reply.Type = MultiReply
		i, err := strconv.Atoi(string(head))
		if err != nil {
			reply.Error = err
		}
		reply.Multi = make([][]byte, i)
		for j := 0; j < i; j++ {
			bulk, err := r.bulk_reply(nil)
			if err != nil {
				reply.Error = err
				break
			}
			reply.Multi[j] = bulk
		}
	}
	return reply, nil
}

func (r *Redis) bulk_reply(head []byte) ([]byte, error) {
	if head == nil {
		data, err := r.Reader.ReadBytes(LF)
		if err != nil {
			return nil, err
		}
		if data[0] != DOLLAR {
			return nil, errors.New("Not bulk head")
		}
		head = data[1 : len(data)-2]
	}
	size, err := strconv.Atoi(string(head))
	if err != nil {
		return nil, err
	}
	if size == -1 {
		return nil, nil
	}
	buf := make([]byte, size+2)
	if _, err := r.Reader.Read(buf); err != nil {
		return nil, err
	} else {
		return buf[:size], nil
	}
}
