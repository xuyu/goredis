package redis

import (
	"strconv"
)

func (r *Redis) SAdd(key string, members ...string) (int, error) {
	if len(members) == 0 {
		return 0, nil
	}
	args := []string{"SADD", key}
	args = append(args, members...)
	if err := r.send_command(args...); err != nil {
		return -1, err
	}
	return r.integer_reply()
}

func (r *Redis) SCard(key string) (int, error) {
	if err := r.send_command("SCARD", key); err != nil {
		return -1, err
	}
	return r.integer_reply()
}

func (r *Redis) SDiff(key string, keys ...string) ([]string, error) {
	args := []string{"SDIFF", key}
	args = append(args, keys...)
	if err := r.send_command(args...); err != nil {
		return []string{}, err
	}
	return r.stringarray_reply()
}

func (r *Redis) SDiffStore(destination, key string, keys ...string) (int, error) {
	args := []string{"SDIFFSTORE", destination, key}
	args = append(args, keys...)
	if err := r.send_command(args...); err != nil {
		return -1, err
	}
	return r.integer_reply()
}

func (r *Redis) SInter(key string, keys ...string) ([]string, error) {
	args := []string{"SINTER", key}
	args = append(args, keys...)
	if err := r.send_command(args...); err != nil {
		return []string{}, err
	}
	return r.stringarray_reply()
}

func (r *Redis) SInterStore(destination, key string, keys ...string) (int, error) {
	args := []string{"SINTERSTORE", destination, key}
	args = append(args, keys...)
	if err := r.send_command(args...); err != nil {
		return -1, err
	}
	return r.integer_reply()
}

func (r *Redis) SIsMember(key, member string) (bool, error) {
	if err := r.send_command("SISMEMBER", key, member); err != nil {
		return false, err
	}
	return r.bool_reply()
}

func (r *Redis) SMembers(key string) ([]string, error) {
	if err := r.send_command("SMEMBERS", key); err != nil {
		return []string{}, err
	}
	return r.stringarray_reply()
}

func (r *Redis) SMove(source, destination, member string) (bool, error) {
	if err := r.send_command("SMOVE", source, destination, member); err != nil {
		return false, err
	}
	return r.bool_reply()
}

func (r *Redis) SPop(key string) (*string, error) {
	if err := r.send_command("SPOP", key); err != nil {
		return nil, err
	}
	return r.bulk_reply()
}

func (r *Redis) SRandomMember(key string, count int) ([]string, error) {
	if err := r.send_command("SRANDOMMEMBER", key, strconv.Itoa(count)); err != nil {
		return []string{}, err
	}
	return r.stringarray_reply()
}

func (r *Redis) SRem(key string, members ...string) (int, error) {
	if len(members) == 0 {
		return 0, nil
	}
	args := []string{"SREM", key}
	args = append(args, members...)
	if err := r.send_command(args...); err != nil {
		return -1, err
	}
	return r.integer_reply()
}

func (r *Redis) SUnion(key string, keys ...string) ([]string, error) {
	args := []string{"SUNION", key}
	args = append(args, keys...)
	if err := r.send_command(args...); err != nil {
		return []string{}, err
	}
	return r.stringarray_reply()
}

func (r *Redis) SUnionStore(destination, key string, keys ...string) (int, error) {
	args := []string{"SUNIONSTORE", destination, key}
	args = append(args, keys...)
	if err := r.send_command(args...); err != nil {
		return -1, err
	}
	return r.integer_reply()
}
