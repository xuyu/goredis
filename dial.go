package goredis

import (
	"errors"
	"net/url"
	"strconv"
	"strings"
	"time"
)

func DialTimeout(network, address string, db int, password string, timeout time.Duration, size int) (*Redis, error) {
	if size < 1 {
		size = 1
	}
	if db < 0 {
		db = 0
	}
	r := &Redis{
		network:  network,
		address:  address,
		db:       db,
		password: password,
		timeout:  timeout,
		size:     size,
		pool:     make(chan *Connection, size),
	}
	for i := 0; i < size; i++ {
		r.pool <- nil
	}
	c, err := r.getConnection()
	if err != nil {
		return nil, err
	}
	r.activeConnection(c)
	return r, nil
}

func DialURL(rawurl string) (*Redis, error) {
	ul, err := url.Parse(rawurl)
	if err != nil {
		return nil, err
	}
	if strings.ToLower(ul.Scheme) != "redis" {
		return nil, errors.New("invalid scheme")
	}
	network := "tcp"
	address := ul.Host
	password := ""
	path := strings.Trim(ul.Path, "/")
	if ul.User != nil {
		if pw, set := ul.User.Password(); set {
			password = pw
		}
	}
	db, err := strconv.Atoi(path)
	if err != nil {
		return nil, err
	}
	timeout, err := time.ParseDuration(ul.Query().Get("timeout"))
	if err != nil {
		return nil, err
	}
	size, err := strconv.Atoi(ul.Query().Get("size"))
	if err != nil {
		return nil, err
	}
	return DialTimeout(network, address, db, password, timeout, size)
}
