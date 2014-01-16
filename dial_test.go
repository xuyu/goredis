package goredis

import (
	"fmt"
	"testing"
)

var format = "tcp://auth:%s@%s/%d?timeout=%s&maxidle=%d"

func TestDialTimeout(t *testing.T) {
	redis, err := DialTimeout(network, address, db, password, timeout, maxidle)
	if err != nil {
		t.Error(err)
	} else if err := redis.Ping(); err != nil {
		t.Error(err)
	}
}

func TestDiaURL(t *testing.T) {
	redis, err := DialURL(fmt.Sprintf(format, password, address, db, timeout.String(), maxidle))
	if err != nil {
		t.Fatal(err)
	} else if err := redis.Ping(); err != nil {
		t.Error(err)
	}
}
