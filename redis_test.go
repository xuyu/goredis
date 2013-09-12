package redis

import (
	"bytes"
	"testing"
	"time"
)

func TestConnect(t *testing.T) {
	r, err := NewClient("tcp4", "127.0.0.1:6379", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
}

func TestErrorReply(t *testing.T) {
	r, err := NewClient("tcp4", "127.0.0.1:6379", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rep := r.Command("AUTH", "nopassword")
	if rep.Type != ErrorReply || rep.Error == nil {
		t.Error(rep)
	}
}

func TestStatusReply(t *testing.T) {
	r, err := NewClient("tcp4", "127.0.0.1:6379", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rep := r.Command("PING")
	if rep.Error != nil || rep.Type != StatusReply {
		t.Error(rep)
	}
	if string(rep.Status) != "PONG" {
		t.Error(string(rep.Status))
	}
}

func TestOKReply(t *testing.T) {
	r, err := NewClient("tcp4", "127.0.0.1:6379", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rep := r.Command("SELECT", 1)
	if rep.Error != nil {
		t.Error(rep)
	}
	if err := rep.OK(); err != nil {
		t.Error(err)
	}
}

func TestIntReply(t *testing.T) {
	r, err := NewClient("tcp4", "127.0.0.1:6379", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rep := r.Command("DBSIZE")
	if rep.Error != nil || rep.Type != IntReply {
		t.Error(rep)
	}
}

func TestBoolReply(t *testing.T) {
	r, err := NewClient("tcp4", "127.0.0.1:6379", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	r.Command("SET", "key", "value")
	rep := r.Command("EXISTS", "key")
	if rep.Error != nil {
		t.Error(rep)
	}
	if b, err := rep.Bool(); err != nil {
		t.Error(err)
	} else if !b {
		t.Error(rep)
	}
}

func TestBulkReply(t *testing.T) {
	r, err := NewClient("tcp4", "127.0.0.1:6379", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rep := r.Command("GET", "nokey")
	if rep.Error != nil {
		t.Error(rep)
	}
	if rep.Type != BulkReply || rep.Bulk != nil {
		t.Error(rep)
	}
	r.Command("SET", "key", "value")
	rep = r.Command("GET", "key")
	if rep.Error != nil || rep.Type != BulkReply || rep.Bulk == nil {
		t.Error(rep)
	}
	if !bytes.Equal(rep.Bulk, []byte("value")) {
		t.Error(rep)
	}
}

func TestMultiReply(t *testing.T) {
	r, err := NewClient("tcp4", "127.0.0.1:6379", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	r.Command("DEL", "key")
	r.Command("SADD", "key", "value1", "value2", "value3")
	rep := r.Command("SMEMBERS", "key")
	if rep.Error != nil || rep.Type != MultiReply || rep.Multi == nil {
		t.Error(rep)
	}
	if len(rep.Multi) != 3 || len(rep.Multi[0]) != 6 {
		t.Error(rep)
	}
}

func TestMapReply(t *testing.T) {
	r, err := NewClient("tcp4", "127.0.0.1:6379", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	r.Command("DEL", "key")
	r.Command("HSET", "key", "k1", "v1")
	r.Command("HSET", "key", "k2", "v2")
	rep := r.Command("HGETALL", "key")
	if rep.Error != nil {
		t.Error(rep.Error)
	}
	if m, err := rep.Map(); err != nil {
		t.Error(err)
	} else if len(m) != 2 {
		t.Error(m)
	} else if !bytes.Equal(m["k1"], []byte("v1")) {
		t.Error(rep)
	}
}
