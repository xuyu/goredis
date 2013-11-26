package redis

import (
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
	_, err := DialTimeout(network, "127.0.0.1:63790", 0, "", 5*time.Second, 5)
	if err == nil {
		t.Error(err)
	}
}

func TestErrorReply(t *testing.T) {
	r, err := dial()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.sendCommand("command_not_exists"); err == nil {
		t.Fatal(err)
	}
}

func TestStatusReply(t *testing.T) {
	r, err := dial()
	if err != nil {
		t.Fatal(err)
	}
	rp, err := r.sendCommand("PING")
	if err != nil {
		t.Fatal(err)
	}
	if rp.Type != StatusReply || rp.Status != "PONG" {
		t.Errorf("%v\n", rp)
	}
}

func TestOKReply(t *testing.T) {
	r, err := dial()
	if err != nil {
		t.Fatal(err)
	}
	rp, err := r.sendCommand("SAVE")
	if err != nil {
		t.Fatal(err)
	}
	if rp.Type != StatusReply || rp.Status != "OK" {
		t.Errorf("%v\n", rp)
	}
}

func TestNumberReply(t *testing.T) {
	r, err := dial()
	if err != nil {
		t.Fatal(err)
	}
	rp, err := r.sendCommand("DBSIZE")
	if err != nil {
		t.Fatal(err)
	}
	if rp.Type != IntegerReply {
		t.Errorf("%v\n", rp)
	}
}

func TestBoolReply(t *testing.T) {
	r, err := dial()
	if err != nil {
		t.Fatal(err)
	}
	r.sendCommand("SET", "key", "value")
	rp, err := r.sendCommand("EXISTS", "key")
	if err != nil {
		t.Fatal(err)
	}
	if rp.Type != IntegerReply || rp.Integer != 1 {
		t.Errorf("%v\n", rp)
	}
}

func TestBulkReply(t *testing.T) {
	r, err := dial()
	if err != nil {
		t.Fatal(err)
	}
	rp, err := r.sendCommand("GET", "nokey")
	if err != nil {
		t.Error(err)
	}
	if rp.Type != BulkReply || rp.Bulk != nil {
		t.Errorf("%v\n", rp)
	}
	r.sendCommand("SET", "key", "value")
	rp, err = r.sendCommand("GET", "key")
	if err != nil {
		t.Error(err)
	}
	if rp.Type != BulkReply || rp.Bulk == nil {
		t.Errorf("%v\n", rp)
	}
	if string(rp.Bulk) != "value" {
		t.Errorf("%v\n", rp)
	}
}

func TestMultiReply(t *testing.T) {
	r, err := dial()
	if err != nil {
		t.Fatal(err)
	}
	r.sendCommand("DEL", "key")
	r.sendCommand("SADD", "key", "value1", "value2", "value3")
	rp, err := r.sendCommand("SMEMBERS", "key")
	if err != nil {
		t.Fatal(err)
	}
	if rp.Type != MultiReply || rp.Multi == nil {
		t.Errorf("%v\n", rp)
	}
	if len(rp.Multi) != 3 || len(rp.Multi[0]) != 6 {
		t.Errorf("%v\n", rp)
	}
}

func TestDiaURL(t *testing.T) {
	rawurl := "redis://127.0.0.1:6379/1?size=5&timeout=10s"
	r, err := DialURL(rawurl)
	if err != nil {
		t.Fatal(err)
	}
	if r.db != 1 || r.size != 5 || r.timeout != 10*time.Second {
		t.Fail()
	}
}

func TestDialURLFail(t *testing.T) {
	rawurl := "redis://tester:password@127.0.0.1:6379/1"
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
