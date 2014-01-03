package goredis

import (
	"testing"
)

func TestAppend(t *testing.T) {
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

func TestGet(t *testing.T) {
	r.Del("key")
	data, err := r.Get("key")
	if err != nil {
		t.Error(err)
	}
	if data != nil {
		t.Fail()
	}
}
