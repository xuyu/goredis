package goredis

import (
	"errors"
	"net/url"
	"strconv"
	"strings"
	"time"
)

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
	maxidle, err := strconv.Atoi(ul.Query().Get("maxidle"))
	if err != nil {
		return nil, err
	}
	return DialTimeout(network, address, db, password, timeout, maxidle)
}
