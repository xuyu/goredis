package redis

import (
	"fmt"
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
	clients, err := r.ClientList()
	if err != nil {
		t.Fatal(err.Error())
	}
	fmt.Println(clients)
}
