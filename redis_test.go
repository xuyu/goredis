package goredis

import (
	"time"
)

var (
	network  = "tcp"
	address  = "192.168.84.250:6379"
	db       = 1
	password = ""
	timeout  = 5 * time.Second
	maxidle  = 1
	r        *Redis
)

func init() {
	client, err := DialTimeout(network, address, db, password, timeout, maxidle)
	if err != nil {
		panic(err)
	}
	r = client
}
