package redis

import (
	"strconv"
)

func (r *Redis) ZAdd(key string, score_members map[int]string) (int, error) {
	if len(score_members) == 0 {
		return 0, nil
	}
	args := []string{"ZADD", key}
	for score, member := range score_members {
		args = append(args, strconv.Itoa(score), member)
	}
	if err := r.send_command(args...); err != nil {
		return -1, err
	}
	return r.integer_reply()
}

func (r *Redis) ZCard(key string) (int, error) {
	if err := r.send_command("ZCARD", key); err != nil {
		return -1, err
	}
	return r.integer_reply()
}

func (r *Redis) ZCount(key, min, max string) (int, error) {
	if err := r.send_command("ZCOUNT", key, min, max); err != nil {
		return -1, err
	}
	return r.integer_reply()
}

func (r *Redis) ZIncrBy(key string, score int, member string) (string, error) {
	if err := r.send_command("ZINCRBY", key, strconv.Itoa(score), member); err != nil {
		return "", err
	}
	return r.string_reply()
}

func (r *Redis) ZRange(key string, start, stop int, withscores bool) ([]string, error) {
	args := []string{"ZRANGE", key, strconv.Itoa(start), strconv.Itoa(stop)}
	if withscores {
		args = append(args, "WITHSCORES")
	}
	if err := r.send_command(args...); err != nil {
		return []string{}, err
	}
	return r.stringarray_reply()
}
