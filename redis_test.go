package redis

import (
	"bytes"
	"testing"
)

func TestConnect(t *testing.T) {
	r, err := Dial("tcp4", "127.0.0.1:6379")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
}

func TestErrorReply(t *testing.T) {
	r, err := Dial("tcp4", "127.0.0.1:6379")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	reply, err := r.SendRecv("AUTH", "nopassword")
	if err != nil {
		t.Fatal(err)
	}
	if reply.Type != ErrorReply || reply.Error == nil {
		t.Error(reply)
	}
}

func TestStatusReply(t *testing.T) {
	r, err := Dial("tcp4", "127.0.0.1:6379")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	reply, err := r.SendRecv("PING")
	if err != nil {
		t.Fatal(err)
	}
	if reply.Error != nil || reply.Type != StatusReply {
		t.Error(reply)
	}
	if reply.Status != "PONG" {
		t.Error(reply.Status)
	}
}

func TestOKReply(t *testing.T) {
	r, err := Dial("tcp4", "127.0.0.1:6379")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err := OK(r.SendRecv("SELECT", 1)); err != nil {
		t.Error(err)
	}
}

func TestIntReply(t *testing.T) {
	r, err := Dial("tcp4", "127.0.0.1:6379")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	reply, err := r.SendRecv("DBSIZE")
	if err != nil {
		t.Fatal(err)
	}
	if reply.Error != nil || reply.Type != IntReply {
		t.Error(reply)
	}
}

func TestBoolReply(t *testing.T) {
	r, err := Dial("tcp4", "127.0.0.1:6379")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	r.SendRecv("SET", "key", "value")
	if b, err := Bool(r.SendRecv("EXISTS", "key")); err != nil {
		t.Error(err)
	} else if !b {
		t.Error(b)
	}
}

func TestBulkReply(t *testing.T) {
	r, err := Dial("tcp4", "127.0.0.1:6379")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	reply, err := r.SendRecv("GET", "nokey")
	if reply.Error != nil {
		t.Error(reply)
	}
	if reply.Type != BulkReply || reply.Bulk != nil {
		t.Error(reply)
	}
	r.SendRecv("SET", "key", "value")
	reply, err = r.SendRecv("GET", "key")
	if reply.Error != nil || reply.Type != BulkReply || reply.Bulk == nil {
		t.Error(reply)
	}
	if !bytes.Equal(reply.Bulk, []byte("value")) {
		t.Error(reply)
	}
}

func TestMultiReply(t *testing.T) {
	r, err := Dial("tcp4", "127.0.0.1:6379")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	r.SendRecv("DEL", "key")
	r.SendRecv("SADD", "key", "value1", "value2", "value3")
	reply, err := r.SendRecv("SMEMBERS", "key")
	if err != nil {
		t.Fatal(err)
	}
	if reply.Error != nil || reply.Type != MultiReply || reply.Multi == nil {
		t.Error(reply)
	}
	if len(reply.Multi) != 3 || len(reply.Multi[0]) != 6 {
		t.Error(reply)
	}
}

func TestMapReply(t *testing.T) {
	r, err := Dial("tcp4", "127.0.0.1:6379")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	r.SendRecv("DEL", "key")
	r.SendRecv("HSET", "key", "k1", "v1")
	r.SendRecv("HSET", "key", "k2", "v2")
	if m, err := Map(r.SendRecv("HGETALL", "key")); err != nil {
		t.Error(err)
	} else if len(m) != 2 {
		t.Error(m)
	} else if !bytes.Equal(m["k1"], []byte("v1")) {
		t.Error(m)
	}
}
