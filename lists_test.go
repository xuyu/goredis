package goredis

import (
	"testing"
)

func TestBLPop(t *testing.T) {
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
