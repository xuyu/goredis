package goredis

import (
	"testing"
)

func TestEval(t *testing.T) {
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
