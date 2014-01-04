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

func TestFlushAll(t *testing.T) {
	if err := r.FlushAll(); err != nil {
		t.Error(err)
	}
}

func TestFlushDB(t *testing.T) {
	if err := r.FlushDB(); err != nil {
		t.Error(err)
	}
}

func TestLastSave(t *testing.T) {
	r.Save()
	if timestamp, err := r.LastSave(); err != nil {
		t.Error(err)
	} else if timestamp <= 0 {
		t.Fail()
	}
}

func TestSave(t *testing.T) {
	if err := r.Save(); err != nil {
		t.Error(err)
	}
}

func TestTime(t *testing.T) {
	tt, err := r.Time()
	if err != nil {
		t.Error(err)
	}
	if len(tt) != 2 {
		t.Fail()
	}
}
