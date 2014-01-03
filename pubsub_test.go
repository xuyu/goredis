package goredis

import (
	"testing"
)

func TestPublish(t *testing.T) {
	if _, err := r.Publish("key", "value"); err != nil {
		t.Error(err)
	}
}
