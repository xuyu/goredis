package redis

import (
	"errors"
	"strconv"
)

var (
	NilBulkError = errors.New("Nil Bulk Reply")
)

func (r *Redis) read_head() ([]byte, []byte, error) {
	data, err := r.reader.ReadBytes(LF)
	if err != nil {
		return []byte{}, []byte{}, err
	}
	return data, data[1 : len(data)-2], nil
}

func (r *Redis) status_reply() (string, error) {
	data, head, err := r.read_head()
	if err != nil {
		return "", err
	}
	if data[0] == PLUS {
		return string(head), nil
	}
	return "", errors.New(string(head))
}

func (r *Redis) ok_reply() error {
	status, err := r.status_reply()
	if err != nil {
		return err
	}
	if status != "OK" {
		return errors.New(status)
	}
	return nil
}

func (r *Redis) bulk_reply() (*string, error) {
	data, head, err := r.read_head()
	if err != nil {
		return nil, err
	}
	if data[0] == DOLLAR {
		size, err := strconv.Atoi(string(head))
		if err != nil {
			return nil, err
		}
		if size == -1 {
			return nil, nil
		}
		buf := make([]byte, size+2)
		if _, err := r.reader.Read(buf); err != nil {
			return nil, err
		}
		bulk := string(buf[:size])
		return &bulk, nil
	}
	if data[0] == COLON {
		bulk := string(head)
		return &bulk, nil
	}
	return nil, errors.New(string(head))
}

func (r *Redis) integer_reply() (int, error) {
	data, head, err := r.read_head()
	if err != nil {
		return -1, err
	}
	if data[0] == COLON {
		n, err := strconv.Atoi(string(head))
		if err != nil {
			return -1, err
		}
		return n, nil
	}
	return -1, errors.New(string(head))
}

func (r *Redis) bool_reply() (bool, error) {
	i, err := r.integer_reply()
	if err != nil {
		return false, err
	}
	if i == 0 {
		return false, nil
	}
	return true, nil
}

func (r *Redis) multibulk_reply() (*[]*string, error) {
	data, head, err := r.read_head()
	if err != nil {
		return nil, err
	}
	if data[0] != STAR {
		return nil, errors.New(string(head))
	}
	n, er := strconv.Atoi(string(head))
	if er != nil {
		return nil, er
	}
	if n == -1 {
		return nil, nil
	}
	result := make([]*string, n)
	for i := 0; i < n; i++ {
		bulk, err := r.bulk_reply()
		if err != nil {
			return nil, err
		}
		result[i] = bulk
	}
	return &result, nil
}

func (r *Redis) stringarray_reply() ([]string, error) {
	result := []string{}
	multibulk, err := r.multibulk_reply()
	if err != nil {
		return result, err
	}
	if multibulk == nil {
		return result, NilBulkError
	}
	for _, p := range *multibulk {
		result = append(result, *p)
	}
	return result, nil
}

func (r *Redis) stringmap_reply() (map[string]string, error) {
	result := make(map[string]string)
	multibulk, err := r.multibulk_reply()
	if err != nil {
		return result, err
	}
	if multibulk == nil {
		return result, NilBulkError
	}
	n := len(*multibulk) / 2
	for i := 0; i < n; i++ {
		result[*(*multibulk)[i*2]] = *(*multibulk)[i*2+1]
	}
	return result, nil
}

func (r *Redis) string_reply() (string, error) {
	bulk, err := r.bulk_reply()
	if err != nil {
		return "", err
	}
	if bulk == nil {
		return "", NilBulkError
	}
	return *bulk, nil
}

func (r *Redis) strparray_reply() ([]*string, error) {
	multibulk, err := r.multibulk_reply()
	if err != nil {
		return []*string{}, err
	}
	if multibulk == nil {
		return []*string{}, NilBulkError
	}
	return *multibulk, nil
}

func (r *Redis) strarrayp_reply() (*[]string, error) {
	multibulk, err := r.multibulk_reply()
	if err != nil {
		return nil, err
	}
	if multibulk == nil {
		return nil, nil
	}
	return &[]string{*(*multibulk)[0], *(*multibulk)[1]}, nil
}
