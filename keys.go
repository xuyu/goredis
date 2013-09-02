package redis

import (
	"strconv"
)

func (r *Redis) Del(keys ...string) (int, error) {
	args := []string{"DEL"}
	args = append(args, keys...)
	if err := r.send_command(args...); err != nil {
		return -1, err
	}
	return r.integer_reply()
}

func (r *Redis) Dump(key string) (string, error) {
	if err := r.send_command("DUMP", key); err != nil {
		return "", err
	}
	bulk, err := r.bulk_reply()
	if err != nil {
		return "", err
	}
	if bulk == nil {
		return "", NilBulkError
	}
	return *bulk, nil
}

func (r *Redis) Exists(key string) (bool, error) {
	if err := r.send_command("EXISTS", key); err != nil {
		return false, err
	}
	return r.bool_reply()
}

func (r *Redis) Expire(key string, seconds int) (bool, error) {
	if err := r.send_command("EXPIRE", key, strconv.Itoa(seconds)); err != nil {
		return false, err
	}
	return r.bool_reply()
}

func (r *Redis) Expireat(key string, timestamp int) (bool, error) {
	if err := r.send_command("EXPIREAT", key, strconv.Itoa(timestamp)); err != nil {
		return false, err
	}
	return r.bool_reply()
}

func (r *Redis) Keys(pattern string) ([]string, error) {
	if err := r.send_command("KEYS", pattern); err != nil {
		return []string{}, err
	}
	multibulk, err := r.multibulk_reply()
	if err != nil {
		return []string{}, err
	}
	if multibulk == nil {
		return []string{}, NilBulkError
	}
	result := make([]string, len(*multibulk))
	for _, key := range *multibulk {
		result = append(result, *key)
	}
	return result, nil
}

func (r *Redis) Move(key string, db int) (bool, error) {
	if err := r.send_command("MOVE", key, strconv.Itoa(db)); err != nil {
		return false, err
	}
	return r.bool_reply()
}

func (r *Redis) Persist(key string) (bool, error) {
	if err := r.send_command("PERSIST", key); err != nil {
		return false, err
	}
	return r.bool_reply()
}

func (r *Redis) Pexpire(key string, milliseconds int) (bool, error) {
	if err := r.send_command("PEXPIRE", key, strconv.Itoa(milliseconds)); err != nil {
		return false, err
	}
	return r.bool_reply()
}

func (r *Redis) Pexpireat(key string, timestamp int) (bool, error) {
	if err := r.send_command("PEXPIREAT", key, strconv.Itoa(timestamp)); err != nil {
		return false, err
	}
	return r.bool_reply()
}

func (r *Redis) Pttl(key string) (int, error) {
	if err := r.send_command("PTTL", key); err != nil {
		return -1, err
	}
	return r.integer_reply()
}

func (r *Redis) RandomKey() (string, error) {
	if err := r.send_command("RANDOMKEY"); err != nil {
		return "", err
	}
	bulk, err := r.bulk_reply()
	if err != nil {
		return "", err
	}
	if bulk == nil {
		return "", nil
	}
	return *bulk, nil
}

func (r *Redis) Rename(key, newkey string) error {
	if err := r.send_command("RENAME", key, newkey); err != nil {
		return err
	}
	_, err := r.status_reply()
	if err != nil {
		return err
	}
	return nil
}

func (r *Redis) Renamenx(key, newkey string) (bool, error) {
	if err := r.send_command("RENAMENX", key, newkey); err != nil {
		return false, err
	}
	return r.bool_reply()
}

func (r *Redis) Restore(key string, ttl int, serialized string) error {
	if err := r.send_command("RESTORE", key, strconv.Itoa(ttl), serialized); err != nil {
		return err
	}
	return r.ok_reply()
}

func (r *Redis) TTL(key string) (int, error) {
	if err := r.send_command("TTL", key); err != nil {
		return -1, err
	}
	return r.integer_reply()
}

func (r *Redis) Type(key string) (string, error) {
	if err := r.send_command("TYPE", key); err != nil {
		return "", err
	}
	return r.status_reply()
}
