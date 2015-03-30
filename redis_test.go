package goredis

import (
	"fmt"
	"testing"
	"time"
)

var (
	network  = "tcp"
	address  = "127.0.0.1:6379"
	db       = 1
	password = ""
	timeout  = 5 * time.Second
	maxidle  = 1
	r        *Redis

	format = "tcp://auth:%s@%s/%d?timeout=%s&maxidle=%d"
)

func init() {
	client, err := Dial(&DialConfig{network, address, db, password, timeout, maxidle})
	if err != nil {
		panic(err)
	}
	r = client
}

func TestDial(t *testing.T) {
	redis, err := Dial(&DialConfig{network, address, db, password, timeout, maxidle})
	if err != nil {
		t.Error(err)
	} else if err := redis.Ping(); err != nil {
		t.Error(err)
	}
	redis.pool.Close()
}

func TestDialTimeout(t *testing.T) {
	redis, err := Dial(&DialConfig{network, address, db, password, timeout, maxidle})
	if err != nil {
		t.Error(err)
	} else if err := redis.Ping(); err != nil {
		t.Error(err)
	}
	redis.pool.Close()
}

func TestDialURL(t *testing.T) {
	redis, err := DialURL(fmt.Sprintf(format, password, address, db, timeout.String(), maxidle))
	if err != nil {
		t.Fatal(err)
	} else if err := redis.Ping(); err != nil {
		t.Error(err)
	}
	redis.pool.Close()
}

func TestNewDialConfigFromURL(t *testing.T) {
	defaultDialConfig := &DialConfig{
		Address:  DefaultAddress,
		Database: 0,
		Network:  DefaultNetwork,
		MaxIdle:  DefaultMaxIdle,
		Password: "",
		Timeout:  DefaultTimeout,
	}

	datasets := []struct {
		errExpected        bool
		expectedDialConfig *DialConfig
		urlString          string
	}{
		{
			false,
			defaultDialConfig,
			"",
		}, {
			true,
			nil,
			"://",
		},
		{
			false,
			defaultDialConfig,
			"tcp://",
		},
		{
			true,
			nil,
			"tcp:///invalid-db",
		},
		{
			true,
			nil,
			"tcp://?maxidle=invalid-maxidle",
		},
		{
			true,
			nil,
			"tcp:///?timeout=invalid-timeout",
		},
		{
			false,
			&DialConfig{
				Address:  address,
				Database: db,
				Network:  "tcp",
				MaxIdle:  maxidle,
				Password: password,
				Timeout:  timeout,
			},
			fmt.Sprintf(format, password, address, db, timeout.String(), maxidle),
		},
	}

	for i, dataset := range datasets {
		expectedDialConfig := dataset.expectedDialConfig
		dialConfig, err := newDialConfigFromURLString(dataset.urlString)

		if err == nil && dataset.errExpected {
			t.Fatalf("Dataset %d: Expected error, but none was returned", i)
		}
		if err != nil {
			if !dataset.errExpected {
				t.Fatalf("Dataset %d: No error expected, but error was returned: %s", i, err.Error())
			}
			continue
		}

		if dialConfig.Address != expectedDialConfig.Address {
			t.Errorf("Dataset %d: Addresses should match. Expected: %s, got: %s", i, expectedDialConfig.Address, dialConfig.Address)
		}
		if dialConfig.Database != expectedDialConfig.Database {
			t.Errorf("Dataset %d: Databases should match. Expected: %d, got: %d", i, expectedDialConfig.Database, dialConfig.Database)
		}
		if dialConfig.MaxIdle != expectedDialConfig.MaxIdle {
			t.Errorf("Dataset %d: MaxIdle should match. Expected: %d, got: %d", i, expectedDialConfig.MaxIdle, dialConfig.MaxIdle)
		}
		if dialConfig.Network != expectedDialConfig.Network {
			t.Errorf("Dataset %d: Networks should match. Expected: %d, got: %d", i, expectedDialConfig.Network, dialConfig.Network)
		}
		if dialConfig.Password != expectedDialConfig.Password {
			t.Errorf("Dataset %d: Passwords should match. Expected: %s, got: %s", i, expectedDialConfig.Password, dialConfig.Password)
		}
		if dialConfig.Timeout != expectedDialConfig.Timeout {
			t.Errorf("Dataset %d: Timeouts should match. Expected: %d, got: %d", i, expectedDialConfig.Timeout, dialConfig.Timeout)
		}
	}
}
