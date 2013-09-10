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

func TestBytesReply(t *testing.T) {
	r.Connect()
	defer r.Close()
	r.SendCommand("SET", "key", "value")
	r.OKReply()
	if err := r.SendCommand("GET", "key"); err != nil {
		t.Fatal(err)
	}
	if b, err := r.BytesReply(); err != nil {
		t.Fatal(err)
	} else if string(b) != "value" {
		t.Fatal(string(b))
	}
}

func TestBulkReply(t *testing.T) {
	r.Connect()
	defer r.Close()
	if err := r.SendCommand("GET", "nokey"); err != nil {
		t.Fatal(err)
	}
	if bulk, err := r.BulkReply(); err != nil {
		t.Fatal(err)
	} else if bulk != nil {
		t.Fatal(bulk)
	}
}

func TestArrayReply(t *testing.T) {
	r.Connect()
	defer r.Close()
	r.SendCommand("DEL", "key")
	r.IntReply()
	r.SendCommand("SADD", "key", "value1", "value2", "value3")
	r.IntReply()
	if err := r.SendCommand("SMEMBERS", "key"); err != nil {
		t.Fatal(err)
	}
	if arr, err := r.ArrayReply(); err != nil {
		t.Fatal(err)
	} else if len(arr) != 3 {
		t.Fatal(arr)
	}
}

func TestMapReply(t *testing.T) {
	r.Connect()
	defer r.Close()
	r.SendCommand("DEL", "key")
	r.IntReply()
	r.SendCommand("HSET", "key", "k1", "v1")
	r.IntReply()
	r.SendCommand("HSET", "key", "k2", "v2")
	r.IntReply()
	if err := r.SendCommand("HGETALL", "key"); err != nil {
		t.Fatal(err)
	}
	if m, err := r.MapReply(); err != nil {
		t.Fatal(err)
	} else if len(m) != 2 {
		t.Fatal(m)
	}
}
