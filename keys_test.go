package goredis

import (
	"testing"
)

func TestExists(t *testing.T) {
	r.Del("key")
	b, err := r.Exists("key")
	if err != nil {
		t.Error(err)
	}
	if b {
		t.Fail()
	}
}

func TestKeys(t *testing.T) {
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
