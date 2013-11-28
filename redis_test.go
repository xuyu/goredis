package redis

import (
	"fmt"
	"testing"
	"time"
)

var (
	network = "tcp"
	address = "192.168.84.250:6379"
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

func TestAppend(t *testing.T) {
	r, _ := dial()
	r.Del("key")
	n, err := r.Append("key", "value")
	if err != nil {
		t.Error(err)
	}
	if n != 5 {
		t.Fail()
	}
	n, err = r.Append("key", "value")
	if err != nil {
		t.Error(err)
	}
	if n != 10 {
		t.Fail()
	}
	r.Del("key")
	r.LPush("key", "value")
	if _, err := r.Append("key", "value"); err == nil {
		t.Error(err)
	}
}

func TestBLPop(t *testing.T) {
	r, _ := dial()
	r.Del("key")
	result, err := r.BLPop([]string{"key"}, 1)
	if err != nil {
		t.Error(err)
	}
	if len(result) != 0 {
		t.Fail()
	}
	r.LPush("key", "value")
	result, err = r.BLPop([]string{"key"}, 0)
	if err != nil {
		t.Error(err)
	}
	if len(result) == 0 {
		t.Fail()
	}
	if result[0] != "key" || result[1] != "value" {
		t.Fail()
	}
}

func TestDBSize(t *testing.T) {
	r, _ := dial()
	r.FlushDB()
	n, err := r.DBSize()
	if err != nil {
		t.Error(err)
	}
	if n != 0 {
		t.Fail()
	}
}

func TestEval(t *testing.T) {
	r, _ := dial()
	rp, err := r.Eval("return {KEYS[1], KEYS[2], ARGV[1], ARGV[2]}", []string{"key1", "key2"}, []string{"arg1", "arg2"})
	if err != nil {
		t.Error(err)
	}
	l, err := rp.ListValue()
	if err != nil {
		t.Error(err)
	}
	if l[0] != "key1" || l[3] != "arg2" {
		t.Fail()
	}
	rp, err = r.Eval("return redis.call('set','foo','bar')", nil, nil)
	if err != nil {
		t.Error(err)
	}
	if err := rp.OKValue(); err != nil {
		t.Error(err)
	}
	rp, err = r.Eval("return 10", nil, nil)
	if err != nil {
		t.Error(err)
	}
	n, err := rp.IntegerValue()
	if err != nil {
		t.Error(err)
	}
	if n != 10 {
		t.Fail()
	}
	rp, err = r.Eval("return {1,2,{3,'Hello World!'}}", nil, nil)
	if err != nil {
		t.Error(err)
	}
	if len(rp.Multi) != 3 {
		t.Fail()
	}
	if rp.Multi[2].Multi[0].Integer != 3 {
		t.Fail()
	}
	if s, err := rp.Multi[2].Multi[1].StringValue(); err != nil || s != "Hello World!" {
		t.Fail()
	}
}

func TestExists(t *testing.T) {
	r, _ := dial()
	r.Del("key")
	b, err := r.Exists("key")
	if err != nil {
		t.Error(err)
	}
	if b {
		t.Fail()
	}
}

func TestGet(t *testing.T) {
	r, _ := dial()
	r.Del("key")
	data, err := r.Get("key")
	if err != nil {
		t.Error(err)
	}
	if data != nil {
		t.Fail()
	}
}

func TestHGetAll(t *testing.T) {
	r, _ := dial()
	r.Del("key")
	pairs := map[string]string{"name": "foo", "attr": "bar"}
	if err := r.HMSet("key", pairs); err != nil {
		t.Error(err)
	}
	data, err := r.HGetAll("key")
	if err != nil {
		t.Error(err)
	}
	if data["name"] != "foo" {
		t.Fail()
	}
}

func TestHMGet(t *testing.T) {
	r, _ := dial()
	r.Del("key")
	r.HSet("key", "field", "value")
	data, err := r.HMGet("key", "field", "nofield")
	if err != nil {
		t.Error(err)
	}
	if string(data[0]) != "value" {
		t.Fail()
	}
	if data[1] != nil {
		t.Fail()
	}
}

func TestKeys(t *testing.T) {
	r, _ := dial()
	r.FlushDB()
	keys, err := r.Keys("*")
	if err != nil {
		t.Error(err)
	}
	if len(keys) != 0 {
		t.Fail()
	}
	r.Set("key", "value", 0, 0, false, false)
	keys, err = r.Keys("*")
	if err != nil {
		t.Error(err)
	}
	if len(keys) != 1 || keys[0] != "key" {
		t.Fail()
	}
}

func TestTransaction(t *testing.T) {
	r, _ := dial()
	transaction, err := r.Transaction()
	if err != nil {
		t.Error(err)
	}
	defer transaction.Close()
	transaction.Command("DEL", "key")
	transaction.Command("SET", "key", 1)
	transaction.Command("INCR", "key")
	transaction.Command("GET", "key")
	result, err := transaction.Exec()
	if err != nil {
		t.Error(err)
	}
	if len(result) != 4 {
		t.Fail()
	}
	if s, err := result[3].StringValue(); err != nil || s != "2" {
		t.Fail()
	}
}

func TestPublish(t *testing.T) {
	r, _ := dial()
	if _, err := r.Publish("key", "value"); err != nil {
		t.Error(err)
	}
}
