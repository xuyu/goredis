package goredis

import (
	"testing"
)

func TestBgRewriteAof(t *testing.T) {
	if err := r.BgRewriteAof(); err != nil {
		t.Error(err)
	}
}

func TestBgSave(t *testing.T) {
	if err := r.BgSave(); err != nil {
		t.Error(err)
	}
}

func TestClientList(t *testing.T) {
	_, err := r.ClientList()
	if err != nil {
		t.Error(err)
	}
}

func TestDBSize(t *testing.T) {
	r.FlushDB()
	n, err := r.DBSize()
	if err != nil {
		t.Error(err)
	}
	if n != 0 {
		t.Fail()
	}
}
