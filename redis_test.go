package redis

import (
	"testing"
)

func TestConnect(t *testing.T) {
	r := &Redis{}
	err := r.Connect()
	if err != nil {
		t.Fatal(err.Error())
	}
}
