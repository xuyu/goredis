package goredis

import (
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultNetwork = "tcp"
	DefaultAddress = ":6379"
	DefaultTimeout = 15 * time.Second
	DefaultMaxIdle = 1
)

type DialConfig struct {
	Network  string
	Address  string
	Database int
	Password string
	Timeout  time.Duration
	MaxIdle  int
}

func Dial(cfg *DialConfig) (*Redis, error) {
	if cfg == nil {
		cfg = &DialConfig{}
	}
	if cfg.Network == "" {
		cfg.Network = DefaultNetwork
	}
	if cfg.Address == "" {
		cfg.Address = DefaultAddress
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = DefaultTimeout
	}
	if cfg.MaxIdle == 0 {
		cfg.MaxIdle = DefaultMaxIdle
	}
	return DialTimeout(cfg.Network, cfg.Address, cfg.Database, cfg.Password, cfg.Timeout, cfg.MaxIdle)
}

func DialTimeout(network, address string, db int, password string, timeout time.Duration, maxidle int) (*Redis, error) {
	r := &Redis{
		network:  network,
		address:  address,
		db:       db,
		password: password,
		timeout:  timeout,
	}
	r.pool = NewConnPool(maxidle, r.NewConnection)
	c, err := r.NewConnection()
	if err != nil {
		return nil, err
	}
	r.pool.Put(c)
	return r, nil
}

func DialURL(rawurl string) (*Redis, error) {
	ul, err := url.Parse(rawurl)
	if err != nil {
		return nil, err
	}
	password := ""
	if ul.User != nil {
		if pw, set := ul.User.Password(); set {
			password = pw
		}
	}
	db, err := strconv.Atoi(strings.Trim(ul.Path, "/"))
	if err != nil {
		return nil, err
	}
	timeout, err := time.ParseDuration(ul.Query().Get("timeout"))
	if err != nil {
		return nil, err
	}
	maxidle, err := strconv.Atoi(ul.Query().Get("maxidle"))
	if err != nil {
		return nil, err
	}
	return DialTimeout(ul.Scheme, ul.Host, db, password, timeout, maxidle)
}
