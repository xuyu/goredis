package redis

import (
	"testing"
)

func TestClientName(t *testing.T) {
	r := initredis()
	defer r.Quit()
	if err := r.ClientSetName("test-connection"); err != nil {
		t.Fatal(err.Error())
	}
	name, err := r.ClientGetName()
	if err != nil {
		t.Fatal(err.Error())
	}
	if *name != "test-connection" {
		t.Fatal(*name)
	}
}

func TestClientList(t *testing.T) {
	r := initredis()
	defer r.Quit()
	_, err := r.ClientList()
	if err != nil {
		t.Fatal(err.Error())
	}
}

func TestDBSize(t *testing.T) {
	r := initredis()
	defer r.Quit()
	if _, err := r.DBSize(); err != nil {
		t.Fatal(err.Error())
	}
}

func TestFlushAll(t *testing.T) {
	r := initredis()
	defer r.Quit()
	if err := r.FlushAll(); err != nil {
		t.Fatal(err.Error())
	}
}

func TestFlushDB(t *testing.T) {
	r := initredis()
	defer r.Quit()
	if err := r.FlushDB(); err != nil {
		t.Fatal(err.Error())
	}
}

func TestInfo(t *testing.T) {
	r := initredis()
	defer r.Quit()
	if _, err := r.Info("default"); err != nil {
		t.Fatal(err.Error())
	}
}

func TestLastSave(t *testing.T) {
	r := initredis()
	defer r.Quit()
	if _, err := r.LastSave(); err != nil {
		t.Fatal(err.Error())
	}
}

func TestTime(t *testing.T) {
	r := initredis()
	defer r.Quit()
	if _, _, err := r.Time(); err != nil {
		t.Fatal(err.Error())
	}
}
