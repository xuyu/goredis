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


Document
--------

- [Redis Commands](http://redis.io/commands)
- [Redis Protocol](http://redis.io/topics/protocol)

- [GoDoc](http://godoc.org/github.com/xuyu/goredis)


Run Test
--------

	go test -test.timeout=10s -test.cpu=4 -cover


License
-------

[The MIT License (MIT) Copyright (c) 2013 xuyu](http://opensource.org/licenses/MIT)
