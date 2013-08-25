package redis

import (
	"fmt"
	"testing"
)

var r = new(Redis)

func init() {
	if err := r.Connect(); err != nil {
		panic(err)
	}
}

func TestAuth(t *testing.T) {
	if err := r.Auth("123"); err != nil {
		fmt.Println(err.Error())
	} else {
		t.Fatal(err.Error())
	}
}

func TestSelect(t *testing.T) {
	if err := r.Select(1); err != nil {
		t.Fatal(err.Error())
	}
	if err := r.Select(65535); err != nil {
		fmt.Println(err.Error())
	} else {
		t.Fatal(err.Error())
	}
}

func TestEcho(t *testing.T) {
	if message, err := r.Echo("Hello"); err != nil {
		t.Fatal(err.Error())
	} else if message != "Hello" {
		t.Fatal(message)
	}
}

func TestPing(t *testing.T) {
	if pong, err := r.Ping(); err != nil {
		t.Fatal(err.Error())
	} else {
		fmt.Println(pong)
	}
}

func TestQuit(t *testing.T) {
	if err := r.Quit(); err != nil {
		t.Fatal(err.Error())
	}
}
