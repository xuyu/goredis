goredis
=======

redis client in golang

[Go or Golang](http://golang.org) is an open source programming language that makes it easy to build simple, reliable, and efficient software.
[Redis](http://redis.io) is an open source, BSD licensed, advanced key-value store. It is often referred to as a data structure server since keys can contain strings, hashes, lists, sets and sorted sets.


Features
--------

* Python Redis Client Like API
* Support Pipeling
* Support Transaction
* Support Publish Subscribe
* Support Lua Eval
* Support Connection Pool
* Support Dial URL-Like

* Support almost all commands
* Support monitor, sort, scan, slowlog .etc

* Pure golang, and no other three-party libraris dependent
* Hight test coverage and will continue to raise
* Tested under Go1.2 and Redis2.8.3


Document
--------

- [Redis Commands](http://redis.io/commands)
- [Redis Protocol](http://redis.io/topics/protocol)

- [GoDoc](http://godoc.org/github.com/xuyu/goredis)


Run Test
--------

	go test -test.timeout=10s -test.cpu=4 -cover


Run Benchmark
-------------

	go test -test.run=none -test.bench="Benchmark.*"

At my virtualbox Ubuntu 13.04 with single CPU: Intel(R) Core(TM) i5-3450 CPU @ 3.10GHz, get result:

	BenchmarkPing	   50000	     40100 ns/op
	BenchmarkLPush	   50000	     34939 ns/op
	BenchmarkLRange	   50000	     41420 ns/op
	BenchmarkGet	   50000	     37948 ns/op
	BenchmarkIncr	   50000	     44460 ns/op
	BenchmarkSet	   50000	     41300 ns/op


License
-------

[The MIT License (MIT) Copyright (c) 2013 xuyu](http://opensource.org/licenses/MIT)
