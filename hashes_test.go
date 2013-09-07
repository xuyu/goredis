package redis

import (
	"testing"
)

func prepaire_hashes(r *Redis) {
	r.HSet("hashes", "key1", "value1")
	r.HSet("hashes", "key2", "value2")
	r.HSet("hashes", "key3", "value3")
}

func TestHDel(t *testing.T) {
	r := initredis()
	defer r.Quit()
	prepaire_hashes(r)
	if n, err := r.HDel("hashes", "key1"); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatalf("hdel return: %d", n)
	}
}

func TestHDelMulti(t *testing.T) {
	r := initredis()
	defer r.Quit()
	prepaire_hashes(r)
	if n, err := r.HDel("hashes", "key1", "key2"); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatalf("hdel multi return: %d", n)
	}
}

func TestHExists(t *testing.T) {
	r := initredis()
	defer r.Quit()
	prepaire_hashes(r)
	if b, err := r.HExists("hashes", "key3"); err != nil {
		t.Fatal(err)
	} else if !b {
		t.Fatal("hexists")
	}
	if b, err := r.HExists("hashes", "keyn"); err != nil {
		t.Fatal(err)
	} else if b {
		t.Fatal("hexists")
	}
}

func TestHGet(t *testing.T) {
	r := initredis()
	defer r.Quit()
	prepaire_hashes(r)
	if v, err := r.HGet("hashes", "key1"); err != nil {
		t.Fatal(err)
	} else if *v != "value1" {
		t.Fatal("hget return: %v", v)
	}
	if v, err := r.HGet("hashes", "keyn"); err != nil {
		t.Fatal(err)
	} else if v != nil {
		t.Fatal("hget return: %s", *v)
	}
}

func TestHGetAll(t *testing.T) {
	r := initredis()
	defer r.Quit()
	prepaire_hashes(r)
	if vs, err := r.HGetAll("hashes"); err != nil {
		t.Fatal(err)
	} else if len(vs) != 3 {
		t.Fatal("hgetall return: %v", vs)
	} else if vs["key1"] != "value1" {
		t.Fatal("hgetall return: %v", vs)
	}
	if vs, err := r.HGetAll("hashesn"); err != nil {
		t.Fatal(err)
	} else if len(vs) != 0 {
		t.Fatal("hgetall return: %v", vs)
	}
}

func TestHKeys(t *testing.T) {
	r := initredis()
	defer r.Quit()
	prepaire_hashes(r)
	if ks, err := r.HKeys("hashes"); err != nil {
		t.Fatal(err)
	} else if len(ks) != 3 {
		t.Fatal("hkeys return: %d %v", len(ks), ks)
	}
	if ks, err := r.HKeys("hashesn"); err != nil {
		t.Fatal(err)
	} else if len(ks) != 0 {
		t.Fatal("hkeys return: %v", ks)
	}
}
