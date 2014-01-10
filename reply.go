package goredis

import (
	"errors"
)

// Reply Type: Status, Integer, Bulk, Multi Bulk
// Error Reply Type return error directly
const (
	ErrorReply = iota
	StatusReply
	IntegerReply
	BulkReply
	MultiReply
)

// Represent Redis Reply
type Reply struct {
	Type    int
	Error   string
	Status  string
	Integer int64  // Support Redis 64bit integer
	Bulk    []byte // Support Redis Null Bulk Reply
	Multi   []*Reply
}

func (rp *Reply) IntegerValue() (int64, error) {
	if rp.Type == ErrorReply {
		return 0, errors.New(rp.Error)
	}
	if rp.Type != IntegerReply {
		return 0, errors.New("invalid reply type, not integer")
	}
	return rp.Integer, nil
}

// Integer replies are also extensively used in order to return true or false.
// For instance commands like EXISTS or SISMEMBER will return 1 for true and 0 for false.
func (rp *Reply) BoolValue() (bool, error) {
	if rp.Type == ErrorReply {
		return false, errors.New(rp.Error)
	}
	if rp.Type != IntegerReply {
		return false, errors.New("invalid reply type, not integer")
	}
	return rp.Integer != 0, nil
}

func (rp *Reply) StatusValue() (string, error) {
	if rp.Type == ErrorReply {
		return "", errors.New(rp.Error)
	}
	if rp.Type != StatusReply {
		return "", errors.New("invalid reply type, not status")
	}
	return rp.Status, nil
}

func (rp *Reply) OKValue() error {
	if rp.Type == ErrorReply {
		return errors.New(rp.Error)
	}
	if rp.Type != StatusReply {
		return errors.New("invalid reply type, not status")
	}
	if rp.Status == "OK" {
		return nil
	}
	return errors.New(rp.Status)
}

func (rp *Reply) BytesValue() ([]byte, error) {
	if rp.Type == ErrorReply {
		return nil, errors.New(rp.Error)
	}
	if rp.Type != BulkReply {
		return nil, errors.New("invalid reply type, not bulk")
	}
	return rp.Bulk, nil
}

func (rp *Reply) StringValue() (string, error) {
	if rp.Type == ErrorReply {
		return "", errors.New(rp.Error)
	}
	if rp.Type != BulkReply {
		return "", errors.New("invalid reply type, not bulk")
	}
	if rp.Bulk == nil {
		return "", nil
	}
	return string(rp.Bulk), nil
}

func (rp *Reply) MultiValue() ([]*Reply, error) {
	if rp.Type == ErrorReply {
		return nil, errors.New(rp.Error)
	}
	if rp.Type != MultiReply {
		return nil, errors.New("invalid reply type, not multi bulk")
	}
	return rp.Multi, nil
}

func (rp *Reply) HashValue() (map[string]string, error) {
	if rp.Type == ErrorReply {
		return nil, errors.New(rp.Error)
	}
	if rp.Type != MultiReply {
		return nil, errors.New("invalid reply type, not multi bulk")
	}
	result := make(map[string]string)
	if rp.Multi != nil {
		length := len(rp.Multi)
		for i := 0; i < length/2; i++ {
			key, err := rp.Multi[i*2].StringValue()
			if err != nil {
				return nil, err
			}
			value, err := rp.Multi[i*2+1].StringValue()
			if err != nil {
				return nil, err
			}
			result[key] = value
		}
	}
	return result, nil
}

func (rp *Reply) ListValue() ([]string, error) {
	if rp.Type == ErrorReply {
		return nil, errors.New(rp.Error)
	}
	if rp.Type != MultiReply {
		return nil, errors.New("invalid reply type, not multi bulk")
	}
	var result []string
	if rp.Multi != nil {
		for _, subrp := range rp.Multi {
			item, err := subrp.StringValue()
			if err != nil {
				return nil, err
			}
			result = append(result, item)
		}
	}
	return result, nil
}

func (rp *Reply) BytesArrayValue() ([][]byte, error) {
	if rp.Type == ErrorReply {
		return nil, errors.New(rp.Error)
	}
	if rp.Type != MultiReply {
		return nil, errors.New("invalid reply type, not multi bulk")
	}
	var result [][]byte
	if rp.Multi != nil {
		for _, subrp := range rp.Multi {
			b, err := subrp.BytesValue()
			if err != nil {
				return nil, err
			}
			result = append(result, b)
		}
	}
	return result, nil
}

func (rp *Reply) BoolArrayValue() ([]bool, error) {
	if rp.Type == ErrorReply {
		return nil, errors.New(rp.Error)
	}
	if rp.Type != MultiReply {
		return nil, errors.New("invalid reply type, not multi bulk")
	}
	var result []bool
	if rp.Multi != nil {
		for _, subrp := range rp.Multi {
			b, err := subrp.BoolValue()
			if err != nil {
				return nil, err
			}
			result = append(result, b)
		}
	}
	return result, nil
}
