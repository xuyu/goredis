package redis

import (
	"bytes"
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
	Status []byte
	Int    int64
	Bulk   []byte
	Multi  [][]byte
}

func (r *Redis) get_reply(rep *Reply) {
	data, err := r.Reader.ReadBytes(LF)
	if err != nil {
		rep.Error = err
	}
	head := data[1 : len(data)-2]
	switch data[0] {
	case MINUS:
		rep.Type = ErrorReply
		rep.Error = errors.New(string(data[1 : len(data)-2]))
	case PLUS:
		rep.Type = StatusReply
		rep.Status = head
	case COLON:
		rep.Type = IntReply
		if i, err := strconv.ParseInt(string(head), 10, 64); err != nil {
			rep.Error = err
		} else {
			rep.Int = i
		}
	case DOLLAR:
		rep.Type = BulkReply
		if bulk, err := r.bulk_reply(head); err != nil {
			rep.Error = err
		} else {
			rep.Bulk = bulk
		}
	case STAR:
		rep.Type = MultiReply
		i, err := strconv.Atoi(string(head))
		if err != nil {
			rep.Error = err
		}
		rep.Multi = make([][]byte, i)
		for j := 0; j < i; j++ {
			bulk, err := r.bulk_reply(nil)
			if err != nil {
				rep.Error = err
				break
			}
			rep.Multi[j] = bulk
		}
	}
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

func (rep *Reply) OK() error {
	if rep.Type != StatusReply {
		return errors.New("Make ok error")
	}
	if bytes.Equal(rep.Status, []byte("OK")) {
		return nil
	}
	return errors.New(string(rep.Status))
}

func (rep *Reply) Bool() (bool, error) {
	if rep.Type != IntReply {
		return false, errors.New("Make bool error")
	}
	if rep.Int == 0 {
		return false, nil
	}
	return true, nil
}

func (rep *Reply) Map() (map[string][]byte, error) {
	if rep.Type != MultiReply || rep.Multi == nil {
		return nil, errors.New("Make map error")
	}
	result := make(map[string][]byte)
	for i := 0; i < len(rep.Multi)/2; i++ {
		key := rep.Multi[i*2]
		if key == nil {
			return nil, errors.New("Nil Bulk Error")
		}
		result[string(key)] = rep.Multi[i*2+1]
	}
	return result, nil
}
