package redis

import (
	"fmt"
	"testing"
	"time"
)

var (
	network = "tcp"
	address = "127.0.0.1:6379"
)

func dial() (*Redis, error) {
	return DialTimeout(network, address, 0, "", 5*time.Second, 5)
}

func TestDial(t *testing.T) {
	_, err := dial()
	if err != nil {
		t.Error(err)
	}
}

func TestDialFail(t *testing.T) {
	_, err := DialTimeout(network, address+"0", 0, "", 5*time.Second, 5)
	if err == nil {
		t.Error(err)
	}
}

func TestDiaURL(t *testing.T) {
	rawurl := fmt.Sprintf("redis://%s/1?size=5&timeout=10s", address)
	r, err := DialURL(rawurl)
	if err != nil {
		t.Fatal(err)
	}
	if r.db != 1 || r.size != 5 || r.timeout != 10*time.Second {
		t.Fail()
	}
}

func TestDialURLFail(t *testing.T) {
	rawurl := fmt.Sprintf("redis://tester:password@%s/1", address)
	_, err := DialURL(rawurl)
	if err == nil {
		t.Fail()
	}
}

func TestAuth(t *testing.T) {
	r, _ := dial()
	if err := r.Auth("password"); err == nil {
		t.Fail()
	}
}

func TestClientList(t *testing.T) {
	r, _ := dial()
	_, err := r.ClientList()
	if err != nil {
		t.Error(err)
	}
}
