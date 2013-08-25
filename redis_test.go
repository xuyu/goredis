package redis

import (
	"testing"
)

func TestConnect(t *testing.T) {
	r := new(Redis)
	err := r.Connect()
	if err != nil {
		t.Fatal(err.Error())
	}
}
