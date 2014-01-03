package goredis

import (
	"testing"
)

func TestTransaction(t *testing.T) {
	transaction, err := r.Transaction()
	if err != nil {
		t.Error(err)
	}
	defer transaction.Close()
	transaction.Command("DEL", "key")
	transaction.Command("SET", "key", 1)
	transaction.Command("INCR", "key")
	transaction.Command("GET", "key")
	result, err := transaction.Exec()
	if err != nil {
		t.Error(err)
	}
	if len(result) != 4 {
		t.Fail()
	}
	if s, err := result[3].StringValue(); err != nil || s != "2" {
		t.Fail()
	}
}
