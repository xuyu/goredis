package redis

import (
	"bytes"
	"errors"
	"strconv"
)

var (
	NilBulkError = errors.New("Nil Bulk Error")
	STATUS_OK    = []byte("OK")
)

func ReplyTypeError(head []byte) error {
	return errors.New(string(head))
}

func (r *Redis) ParseHead() ([]byte, error) {
	data, err := r.Reader.ReadBytes(LF)
	if err != nil {
		return data, err
	}
	if data[0] == MINUS {
		return data, errors.New(string(data[1 : len(data)-2]))
	}
	return data[:len(data)-2], nil
}

func (r *Redis) StatusReply() ([]byte, error) {
	head, err := r.ParseHead()
	if err != nil {
		return head, err
	}
	if head[0] != PLUS {
		return head, ReplyTypeError(head)
	}
	return head[1:], nil
}

func (r *Redis) OKReply() error {
	if status, err := r.StatusReply(); err != nil {
		return err
	} else if !bytes.Equal(status, STATUS_OK) {
		return errors.New(string(status))
	}
	return nil
}

func (r *Redis) BulkReply() (*[]byte, error) {
	head, err := r.ParseHead()
	if err != nil {
		return nil, err
	}
	if head[0] != DOLLAR {
		return nil, ReplyTypeError(head)
	}
	size, err := strconv.Atoi(string(head[1:]))
	if err != nil {
		return nil, err
	}
	if size == -1 {
		return nil, nil
	}
	buf := make([]byte, size+2)
	if _, err := r.Reader.Read(buf); err != nil {
		return nil, err
	}
	bulk := buf[:size]
	return &bulk, nil
}

func (r *Redis) BytesReply() ([]byte, error) {
	bulk, err := r.BulkReply()
	if err != nil {
		return []byte{}, err
	}
	if bulk == nil {
		return []byte{}, NilBulkError
	}
	return *bulk, nil
}

func (r *Redis) IntReply() (int64, error) {
	head, err := r.ParseHead()
	if err != nil {
		return -1, err
	}
	if head[0] != COLON {
		return -1, ReplyTypeError(head)
	}
	n, err := strconv.ParseInt(string(head[1:]), 10, 64)
	if err != nil {
		return -1, err
	}
	return n, nil
}

func (r *Redis) BoolReply() (bool, error) {
	i, err := r.IntReply()
	if err != nil {
		return false, err
	}
	if i == 0 {
		return false, nil
	}
	return true, nil
}

func (r *Redis) MultiBulkReply() (*[]*[]byte, error) {
	head, err := r.ParseHead()
	if err != nil {
		return nil, err
	}
	if head[0] != STAR {
		return nil, ReplyTypeError(head)
	}
	n, er := strconv.Atoi(string(head[1:]))
	if er != nil {
		return nil, er
	}
	if n == -1 {
		return nil, nil
	}
	result := make([]*[]byte, n)
	for i := 0; i < n; i++ {
		bulk, err := r.BulkReply()
		if err != nil {
			return nil, err
		}
		result[i] = bulk
	}
	return &result, nil
}

func (r *Redis) ArrayReply() ([][]byte, error) {
	multibulk, err := r.MultiBulkReply()
	result := [][]byte{}
	if err != nil {
		return result, err
	}
	if multibulk == nil {
		return result, NilBulkError
	}
	for _, p := range *multibulk {
		if p == nil {
			return result, NilBulkError
		}
		result = append(result, *p)
	}
	return result, nil
}

func (r *Redis) MapReply() (map[string][]byte, error) {
	multibulk, err := r.MultiBulkReply()
	result := make(map[string][]byte)
	if err != nil {
		return result, err
	}
	if multibulk == nil {
		return result, NilBulkError
	}
	n := len(*multibulk) / 2
	for i := 0; i < n; i++ {
		key := (*multibulk)[i*2]
		value := (*multibulk)[i*2+1]
		if key == nil || value == nil {
			return result, NilBulkError
		}
		result[string(*key)] = *value
	}
	return result, nil
}
