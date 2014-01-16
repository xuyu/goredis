package goredis

import (
	"fmt"
	"testing"
)

var format = "redis://auth:%s@%s/%d?maxidle=%d&timeout=%s"

func TestDialFail(t *testing.T) {
	_, err := DialTimeout(network, address+"0", db, password, timeout, 0)
	if err == nil {
		t.Error(err)
	}
}

func TestDiaURL(t *testing.T) {
	r, err := DialURL(fmt.Sprintf(format, password, address, 0, 1, timeout.String()))
	if err != nil {
		t.Fatal(err)
	}
	if r.db != 0 || r.timeout != timeout {
		t.Fail()
	}
}

func TestDialURLFail(t *testing.T) {
	_, err := DialURL(fmt.Sprintf("tcp://%s/%d?maxidle=%d&timeout=%s", address, db, maxidle, timeout.String()))
	if err == nil {
		t.Error("1")
	}
	_, err = DialURL(fmt.Sprintf("redis://auth:%s@%s/%d?maxidle=%d&timeout=%s", password+"password", address, db, maxidle, timeout.String()))
	if err == nil {
		t.Error("2")
	}
	_, err = DialURL(fmt.Sprintf("redis://%s/?maxidle=%d&timeout=%s", address, maxidle, timeout.String()))
	if err == nil {
		t.Error("3")
	}
	_, err = DialURL(fmt.Sprintf("redis://%s/%d", address, db))
	if err == nil {
		t.Error("4")
	}
}
