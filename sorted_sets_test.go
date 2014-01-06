package goredis

import (
	"testing"
)

func TestZAdd(t *testing.T) {
	r.Del("key")
	pairs := map[string]float64{
		"one":   1.0,
		"two":   1.0,
		"three": 3.0,
	}
	if n, err := r.ZAdd("key", pairs); err != nil {
		t.Error(err)
	} else if n != 3 {
		t.Fail()
	}
	if n, _ := r.ZAdd("key", map[string]float64{"two": 2.0}); n != 0 {
		t.Fail()
	}
}

func TestZCard(t *testing.T) {
	r.Del("key")
	pairs := map[string]float64{
		"one":   1.0,
		"two":   1.0,
		"three": 3.0,
	}
	r.ZAdd("key", pairs)
	if n, err := r.ZCard("key"); err != nil {
		t.Error(err)
	} else if n != 3 {
		t.Fail()
	}
}

func TestZCount(t *testing.T) {
	r.Del("key")
	pairs := map[string]float64{
		"one":   1.0,
		"two":   2.0,
		"three": 3.0,
	}
	r.ZAdd("key", pairs)
	if n, err := r.ZCount("key", "-inf", "+inf"); err != nil {
		t.Error(err)
	} else if n != 3 {
		t.Fail()
	}
	if n, _ := r.ZCount("key", "(1", "3"); n != 2 {
		t.Fail()
	}
}

func TestZIncrBy(t *testing.T) {
	r.Del("key")
	pairs := map[string]float64{
		"one":   1.0,
		"two":   1.0,
		"three": 3.0,
	}
	r.ZAdd("key", pairs)
	if n, err := r.ZIncrBy("key", 1.0, "two"); err != nil {
		t.Error(err)
	} else if n != 2.0 {
		t.Fail()
	}
}

func TestZRange(t *testing.T) {
	r.Del("key")
	pairs := map[string]float64{
		"one":   1.0,
		"two":   2.0,
		"three": 3.0,
	}
	r.ZAdd("key", pairs)
	if result, err := r.ZRange("key", 0, -1, false); err != nil {
		t.Error(err)
	} else if len(result) != 3 {
		t.Fail()
	} else if result[0] != "one" {
		t.Fail()
	}
	if result, err := r.ZRange("key", -2, -1, true); err != nil {
		t.Error(err)
	} else if len(result) != 4 {
		t.Fail()
	} else if result[0] != "two" {
		t.Fail()
	} else if result[1] != "2" {
		t.Fail()
	}
}
