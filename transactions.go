package redis

func (r *Redis) Discard() error {
	if err := r.send_command("DISCARD"); err != nil {
		return err
	}
	return r.ok_reply()
}

func (r *Redis) Exec() (*[]*string, error) {
	if err := r.send_command("EXEC"); err != nil {
		return nil, err
	}
	return r.multibulk_reply()
}

func (r *Redis) Multi() error {
	if err := r.send_command("MULTI"); err != nil {
		return err
	}
	return r.ok_reply()
}

func (r *Redis) UnWatch() error {
	if err := r.send_command("UNWATCH"); err != nil {
		return err
	}
	return r.ok_reply()
}

func (r *Redis) Watch(keys ...string) error {
	args := []string{"WATCH"}
	args = append(args, keys...)
	if err := r.send_command(args...); err != nil {
		return err
	}
	return r.ok_reply()
}
