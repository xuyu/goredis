package redis

import (
	"strconv"
)

func (r *Redis) Auth(password string) error {
	if err := r.send_command("AUTH", password); err != nil {
		return err
	}
	if err := r.ok_reply(); err != nil {
		return err
	}
	r.password = password
	return nil
}

func (r *Redis) Select(db int) error {
	if err := r.send_command("SELECT", strconv.Itoa(db)); err != nil {
		return err
	}
	if _, err := r.status_reply(); err != nil {
		return err
	}
	r.database = db
	return nil
}

func (r *Redis) Echo(message string) (string, error) {
	if err := r.send_command("ECHO", message); err != nil {
		return "", err
	}
	m, err := r.bulk_reply()
	if err != nil {
		return "", err
	}
	if m == nil {
		return "", NilBulkError
	}
	return *m, nil
}

func (r *Redis) Ping() (string, error) {
	if err := r.send_command("PING"); err != nil {
		return "", err
	}
	status, err := r.status_reply()
	if err != nil {
		return "", err
	}
	return status, nil
}

func (r *Redis) Quit() error {
	if err := r.send_command("QUIT"); err != nil {
		return err
	}
	if err := r.ok_reply(); err != nil {
		return err
	}
	r.conn.Close()
	return nil
}
