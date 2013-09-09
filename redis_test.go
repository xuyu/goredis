package redis

import (
	"testing"
	"time"
)

var r = NewClient("tcp4", "127.0.0.1:6379", 10*time.Second)

func TestConnect(t *testing.T) {
	if err := r.Connect(); err != nil {
		t.Fatal(err)
	}
	r.Close()
}

func TestErrorReply(t *testing.T) {
	r.Connect()
	defer r.Close()
	if err := r.SendCommand("AUTH", "nopassword"); err != nil {
		t.Fatal(err)
	}
	if status, err := r.StatusReply(); err == nil {
		t.Fatal(status)
	} else {
		t.Log(status)
	}
}

func TestStatusReply(t *testing.T) {
	r.Connect()
	defer r.Close()
	if err := r.SendCommand("PING"); err != nil {
		t.Fatal(err)
	}
	if status, err := r.StatusReply(); err != nil {
		t.Fatal(err)
	} else if string(status) != "PONG" {
		t.Fatal(string(status))
	}
}

func TestOKReply(t *testing.T) {
	r.Connect()
	defer r.Close()
	if err := r.SendCommand("SELECT", 1); err != nil {
		t.Fatal(err)
	}
	if err := r.OKReply(); err != nil {
		t.Fatal(err)
	}
}

func TestIntReply(t *testing.T) {
	r.Connect()
	defer r.Close()
	if err := r.SendCommand("DBSIZE"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.IntReply(); err != nil {
		t.Fatal(err)
	}
}

func TestBoolReply(t *testing.T) {
	r.Connect()
	defer r.Close()
	r.SendCommand("SET", "key", "value")
	r.OKReply()
	if err := r.SendCommand("EXISTS", "key"); err != nil {
		t.Fatal(err)
	}
	if b, err := r.BoolReply(); err != nil {
		t.Fatal(err)
	} else if !b {
		t.Fatal(b)
	}
}
