package redis

import (
	"strings"
)

func (r *Redis) BgRewriteAOF() error {
	if err := r.send_command("BGREWRITEAOF"); err != nil {
		return err
	}
	return r.ok_reply()
}

func (r *Redis) BgSave() error {
	if err := r.send_command("BGSAVE"); err != nil {
		return err
	}
	return r.ok_reply()
}

func (r *Redis) ClientGetName() (*string, error) {
	if err := r.send_command("CLIENT", "GETNAME"); err != nil {
		return nil, err
	}
	if bulk, err := r.bulk_reply(); err != nil {
		return nil, err
	} else {
		return bulk, nil
	}
}

func (r *Redis) ClientKill(ip, port string) error {
	if err := r.send_command("CLIENT", "KILL", ip+":"+port); err != nil {
		return err
	}
	return r.ok_reply()
}

func (r *Redis) ClientList() ([]map[string]string, error) {
	clients := []map[string]string{}
	if err := r.send_command("CLIENT", "LIST"); err != nil {
		return clients, err
	}
	bulk, err := r.bulk_reply()
	if err != nil {
		return clients, err
	}
	if bulk == nil {
		return clients, NilBulkError
	}
	delim := string([]byte{LF})
	for _, line := range strings.Split(strings.Trim(*bulk, delim), delim) {
		m := make(map[string]string)
		for _, field := range strings.Fields(line) {
			sr := strings.Split(field, "=")
			m[sr[0]] = sr[1]
		}
		clients = append(clients, m)
	}
	return clients, nil
}

func (r *Redis) ClientSetName(name string) error {
	if err := r.send_command("CLIENT", "SETNAME", name); err != nil {
		return err
	}
	return r.ok_reply()
}

func (r *Redis) ConfigGet(pattern string) (*string, error) {
	if err := r.send_command("CONFIG", "GET", pattern); err != nil {
		return nil, err
	}
	return r.bulk_reply()
}

func (r *Redis) ConfigResetStat() error {
	if err := r.send_command("CONFIG", "RESETSTAT"); err != nil {
		return err
	}
	return r.ok_reply()
}
