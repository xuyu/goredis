package goredis

import (
	"fmt"
	"testing"
)

var format = "redis://auth:%s@%s/%d?size=%d&timeout=%s"

func TestDialFail(t *testing.T) {
	_, err := DialTimeout(network, address+"0", db, password, timeout, pool)
	if err == nil {
		t.Error(err)
	}
}

func TestDiaURL(t *testing.T) {
	r, err := DialURL(fmt.Sprintf(format, password, address, db, pool, timeout.String()))
	if err != nil {
		t.Fatal(err)
	}
	if r.db != db || r.size != pool || r.timeout != timeout {
		t.Fail()
	}
}

func TestDialURLFail(t *testing.T) {
	_, err := DialURL(fmt.Sprintf("tcp://%s/%d?size=%d&timeout=%s", address, db, pool, timeout.String()))
	if err == nil {
		t.Error("1")
	}
	_, err = DialURL(fmt.Sprintf("redis://auth:%s@%s/%d?size=%d&timeout=%s", password+"password", address, db, pool, timeout.String()))
	if err == nil {
		t.Error("2")
	}
	_, err = DialURL(fmt.Sprintf("redis://%s/?size=%d&timeout=%s", address, pool, timeout.String()))
	if err == nil {
		t.Error("3")
	}
	_, err = DialURL(fmt.Sprintf("redis://%s/%d", address, db))
	if err == nil {
		t.Error("4")
	}
}
