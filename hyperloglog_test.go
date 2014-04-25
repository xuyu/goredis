package goredis

import (
	"testing"
)

func TestPFAdd(t *testing.T) {
	r.Del("hll")
	n, err := r.PFAdd("hll", "a", "b")
	if err != nil {
		t.Error(err.Error())
	}
	if n != 1 {
		t.Fail()
	}
}

func TestPFCount(t *testing.T) {
	r.Del("hll", "hll2")
	r.PFAdd("hll", "1", "2")
	r.PFAdd("hll2", "a", "1")
	n, err := r.PFCount("hll")
	if err != nil {
		t.Error(err.Error())
	}
	if n != 2 {
		t.Fail()
	}
	n, _ = r.PFCount("hll", "hll2")
	if n != 3 {
		t.Fail()
	}
}

func TestPFMerge(t *testing.T) {
	r.Del("hll", "hll2")
	r.PFAdd("hll", "a")
	r.PFAdd("hll2", "1")
	if err := r.PFMerge("hll3", "hll", "hll2"); err != nil {
		t.Error(err.Error())
	}
}
