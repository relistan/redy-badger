redy-badger
===========

A fun proof of concept that mashes up some great Go libraries. It's a Redis
protocol compatible server that uses
[badger](https://github.com/dgraph-io/badger) as its storage backend. It
currently also implements a subset of the Bloom filter commands and Cuckoo
filter commands also serialized to/from Badger. The frontend is
[Redeo](https://github.com/bsm/redeo).

Just a thought experiment, not a production thing.

## Usage

### Basics
```
$ redis-cli
127.0.0.1:6379> set asdf asdfasdf
OK
127.0.0.1:6379> get asdf
"asdfasdf"
127.0.0.1:6379> del asdf
OK
127.0.0.1:6379>
```

### Keys and key ranges
```
$ redis-cli
127.0.0.1:6379> keys
1) "asdf"
2) "asdfasdf"
3) "asdfasdfasdasasdfdff"
4) "asdfasdfasdasdff"
5) "asdfasdfasdf"
6) "bloom"
7) "qwer"
127.0.0.1:6379> keys asd
1) "asdf"
2) "asdfasdf"
3) "asdfasdfasdasasdfdff"
4) "asdfasdfasdasdff"
5) "asdfasdfasdf"
127.0.0.1:6379>
```

### Bloom filters
```
$ redis-cli
127.0.0.1:6379> bf.insert bloom capacity 50000 error 0.1
OK
127.0.0.1:6379> bf.add bloom +13164647972
OK
127.0.0.1:6379> bf.add bloom +353873549906
OK
127.0.0.1:6379> bf.add bloom +4401483123445
OK
127.0.0.1:6379> bf.card bloom
(integer) 3
127.0.0.1:6379> bf.exists bloom +353873549906
(integer) 1
127.0.0.1:6379> bf.exists bloom +4401483123445
(integer) 1
127.0.0.1:6379> bf.exists bloom yomomma
(integer) 0
127.0.0.1:6379> quit

... persistence to disk ...

$ redis-cli
127.0.0.1:6379> bf.exists bloom +4401483123445
(integer) 1
```

### Cuckoo filters
```
$ redis-cli
127.0.0.1:6379> cf.insert cuckoo capacity 50000 error 0.1
OK
127.0.0.1:6379> cf.add cuckoo +13164647972
OK
127.0.0.1:6379> cf.add cuckoo +353873549906
OK
127.0.0.1:6379> cf.add cuckoo +4401483123445
OK
127.0.0.1:6379> cf.card cuckoo
(integer) 3
127.0.0.1:6379> cf.exists cuckoo +353873549906
(integer) 1
127.0.0.1:6379> cf.exists cuckoo +4401483123445
(integer) 1
127.0.0.1:6379> cf.exists cuckoo yomomma
(integer) 0
127.0.0.1:6379> cf.del cuckoo +353873549906
(integer) 1
127.0.0.1:6379> quit

... persistence to disk ...

$ redis-cli
127.0.0.1:6379> cf.exists cuckoo +4401483123445
(integer) 1
```
