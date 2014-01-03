package goredis

import (
	"testing"
)

func TestHGetAll(t *testing.T) {
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
