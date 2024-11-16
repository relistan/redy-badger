redy-badger
===========

A fun proof of concept that mashes up some great Go libraries. It's a Redis
compatible server that uses [badger](https://github.com/dgraph-io/badger) as
its storage backend. It currently also implements a subset of the Bloom filter
commands, also serialized to/from Badger. The frontend is [Redeo](https://github.com/bsm/redeo).

Just a thought experiment, not a production thing.
