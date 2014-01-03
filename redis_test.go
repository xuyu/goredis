package goredis

import (
	"time"
)

var (
	network  = "tcp"
	address  = "192.168.84.250:6379"
	db       = 0
	password = ""
	timeout  = 5 * time.Second
	pool     = 5
	r        *Redis
)

func init() {
	client, err := DialTimeout(network, address, db, password, timeout, pool)
	if err != nil {
		panic(err)
	}
	r = client
}
