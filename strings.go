package redis

import (
	"strconv"
)

func (r *Redis) Append(key, value string) (int, error) {
	if err := r.send_command("APPEND", key, value); err != nil {
		return -1, err
	}
	return r.integer_reply()
}

func (r *Redis) BitCount(key, start, end string) (int, error) {
	args := []string{"BITCOUNT", key}
	if start != "" {
		args = append(args, start)
	}
	if end != "" {
		args = append(args, end)
	}
	if err := r.send_command(args...); err != nil {
		return -1, err
	}
	return r.integer_reply()
}

func (r *Redis) BitOP(operation, destkey string, keys ...string) (int, error) {
	args := []string{"BITOP", operation, destkey}
	args = append(args, keys...)
	if err := r.send_command(args...); err != nil {
		return -1, err
	}
	return r.integer_reply()
}

func (r *Redis) Decr(key string) (int, error) {
	if err := r.send_command("DECR", key); err != nil {
		return -1, err
	}
	return r.integer_reply()
}

func (r *Redis) DecrBy(key string, decrement int) (int, error) {
	if err := r.send_command("DECRBY", key, strconv.Itoa(decrement)); err != nil {
		return -1, err
	}
	return r.integer_reply()
}

func (r *Redis) Get(key string) (*string, error) {
	if err := r.send_command("GET", key); err != nil {
		return nil, err
	}
	return r.bulk_reply()
}

func (r *Redis) GetBit(key string, offset int) (int, error) {
	if err := r.send_command("GETBIT", key, strconv.Itoa(offset)); err != nil {
		return -1, err
	}
	return r.integer_reply()
}

func (r *Redis) GetRange(key string, start, end int) (string, error) {
	if err := r.send_command("GETRANGE", key, strconv.Itoa(start), strconv.Itoa(end)); err != nil {
		return "", err
	}
	return r.string_reply()
}

func (r *Redis) GetSet(key, value string) (string, error) {
	if err := r.send_command("GETSET", key, value); err != nil {
		return "", err
	}
	return r.string_reply()
}

func (r *Redis) Incr(key string) (int, error) {
	if err := r.send_command("INCR", key); err != nil {
		return -1, err
	}
	return r.integer_reply()
}

func (r *Redis) IncrBy(key string, increment int) (int, error) {
	if err := r.send_command("INCRBY", key, strconv.Itoa(increment)); err != nil {
		return -1, err
	}
	return r.integer_reply()
}

func (r *Redis) IncrByFloat(key string, increment string) (string, error) {
	if err := r.send_command("INCRBYFLOAT", key, increment); err != nil {
		return "", err
	}
	return r.string_reply()
}

func (r *Redis) MGet(key string, keys ...string) ([]*string, error) {
	args := []string{"MGET", key}
	args = append(args, keys...)
	if err := r.send_command(args...); err != nil {
		return []*string{}, err
	}
	return r.strparray_reply()
}

func (r *Redis) MSet(keyvalues map[string]string) error {
	args := []string{"MSET"}
	for key, value := range keyvalues {
		args = append(args, key, value)
	}
	if err := r.send_command(args...); err != nil {
		return err
	}
	return r.ok_reply()
}

func (r *Redis) MSetnx(keyvalues map[string]string) (bool, error) {
	args := []string{"MSETNX"}
	for key, value := range keyvalues {
		args = append(args, key, value)
	}
	if err := r.send_command(args...); err != nil {
		return false, err
	}
	return r.bool_reply()
}

func (r *Redis) PSetex(key string, milliseconds int, value string) error {
	if err := r.send_command("PSETEX", key, strconv.Itoa(milliseconds), value); err != nil {
		return err
	}
	return r.ok_reply()
}

func (r *Redis) Set(key, value string) error {
	if err := r.send_command("SET", key, value); err != nil {
		return err
	}
	return r.ok_reply()
}

func (r *Redis) SetBit(key string, offset int, value int) (int, error) {
	if err := r.send_command("SETBIT", strconv.Itoa(offset), strconv.Itoa(value)); err != nil {
		return -1, err
	}
	return r.integer_reply()
}

func (r *Redis) Setex(key string, seconds int, value string) error {
	if err := r.send_command("SETEX", key, strconv.Itoa(seconds), value); err != nil {
		return err
	}
	return r.ok_reply()
}

func (r *Redis) Setnx(key, value string) (bool, error) {
	if err := r.send_command("SETNX", key, value); err != nil {
		return false, err
	}
	return r.bool_reply()
}

func (r *Redis) SetRange(key string, offset int, value string) (int, error) {
	if err := r.send_command("SETRANGE", key, strconv.Itoa(offset), value); err != nil {
		return -1, err
	}
	return r.integer_reply()
}

func (r *Redis) StrLen(key string) (int, error) {
	if err := r.send_command("STRLEN", key); err != nil {
		return -1, err
	}
	return r.integer_reply()
}
