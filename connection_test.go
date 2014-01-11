package goredis

import (
	"testing"
)

func TestAuth(t *testing.T) {
	if err := r.Auth("I am not password"); err == nil {
		t.Fail()
	}
}

func TestEcho(t *testing.T) {
	msg := "message"
	if ret, err := r.Echo(msg); err != nil {
		t.Error(err)
	} else if ret != msg {
		t.Errorf("echo %s\n%s", msg, ret)
	}
}

func TestPing(t *testing.T) {
	if err := r.Ping(); err != nil {
		t.Error(err)
	}
}

func TestQuit(t *testing.T) {
	client, err := DialTimeout(network, address, db, password, timeout, pool)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Quit(); err != nil {
		t.Error(err)
	}
}

func TestSelect(t *testing.T) {
	client, err := DialTimeout(network, address, db, password, timeout, pool)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Select(db + 1); err != nil {
		t.Error(err)
	}
}
