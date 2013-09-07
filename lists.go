package redis

import (
	"errors"
	"fmt"
	"strconv"
)

func (r *Redis) BLPop(keys []string, timeout int) (*[]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	args := []string{"BLPOP"}
	args = append(args, keys...)
	if err := r.send_command(args...); err != nil {
		return nil, err
	}
	return r.strarrayp_reply()
}

func (r *Redis) BRPop(keys []string, timeout int) (*[]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	args := []string{"BRPOP"}
	args = append(args, keys...)
	if err := r.send_command(args...); err != nil {
		return nil, err
	}
	return r.strarrayp_reply()
}

func (r *Redis) BRPopLPush(source, destination string, timeout int) (*string, error) {
	if err := r.send_command("BRPOPLPUSH", source, destination, strconv.Itoa(timeout)); err != nil {
		return nil, err
	}
	return r.bulk_reply()
}

func (r *Redis) LIndex(key string, index int) (*string, error) {
	if err := r.send_command("LINDEX", key, strconv.Itoa(index)); err != nil {
		return nil, err
	}
	return r.bulk_reply()
}

func (r *Redis) LInsert(key, pos, pivot, value string) (int, error) {
	if pos != "BEFORE" || pos != "AFTER" {
		return -1, errors.New(fmt.Sprintf("Invalid pos: %s", pos))
	}
	if err := r.send_command("LINSERT", key, pos, pivot, value); err != nil {
		return -1, err
	}
	return r.integer_reply()
}

func (r *Redis) LLen(key string) (int, error) {
	if err := r.send_command("LLEN", key); err != nil {
		return -1, err
	}
	return r.integer_reply()
}

func (r *Redis) LPop(key string) (*string, error) {
	if err := r.send_command("LPOP", key); err != nil {
		return nil, err
	}
	return r.bulk_reply()
}

func (r *Redis) LPush(key string, values ...string) (int, error) {
	if len(values) == 0 {
		return -1, errors.New("Empty values")
	}
	args := []string{"LPUSH", key}
	args = append(args, values...)
	if err := r.send_command(args...); err != nil {
		return -1, err
	}
	return r.integer_reply()
}

func (r *Redis) LPushx(key, value string) (int, error) {
	if err := r.send_command("LPUSHX", key, value); err != nil {
		return -1, err
	}
	return r.integer_reply()
}

func (r *Redis) LRange(key string, start, stop int) ([]string, error) {
	if err := r.send_command("LRANGE", key, strconv.Itoa(start), strconv.Itoa(stop)); err != nil {
		return []string{}, err
	}
	return r.stringarray_reply()
}

func (r *Redis) LRem(key string, count int, value string) (int, error) {
	if err := r.send_command("LREM", key, strconv.Itoa(count), value); err != nil {
		return -1, err
	}
	return r.integer_reply()
}

func (r *Redis) LSet(key string, index int, value string) error {
	if err := r.send_command("LSET", key, strconv.Itoa(index), value); err != nil {
		return err
	}
	return r.ok_reply()
}

func (r *Redis) LTrim(key string, start, stop int) error {
	if err := r.send_command("LTRIM", key, strconv.Itoa(start), strconv.Itoa(stop)); err != nil {
		return err
	}
	return r.ok_reply()
}

func (r *Redis) RPop(key string) (*string, error) {
	if err := r.send_command("RPOP", key); err != nil {
		return nil, err
	}
	return r.bulk_reply()
}

func (r *Redis) RPopLPush(source, destination string) (*string, error) {
	if err := r.send_command("RPOPLPUSH", source, destination); err != nil {
		return nil, err
	}
	return r.bulk_reply()
}

func (r *Redis) RPush(key string, values ...string) (int, error) {
	if len(values) == 0 {
		return -1, errors.New("Empty values")
	}
	args := []string{"RPUSH", key}
	args = append(args, values...)
	if err := r.send_command(args...); err != nil {
		return -1, err
	}
	return r.integer_reply()
}

func (r *Redis) RPushx(key, value string) (int, error) {
	if err := r.send_command("RPUSHX", key, value); err != nil {
		return -1, err
	}
	return r.integer_reply()
}
