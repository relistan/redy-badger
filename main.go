package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"net"
	"strconv"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/bsm/redeo"
	"github.com/bsm/redeo/resp"
	"github.com/dgraph-io/badger/v4"
	cuckoo "github.com/seiflotfy/cuckoofilter"
	log "github.com/sirupsen/logrus"
)

var flags struct {
	addr string
}

func init() {
	flag.StringVar(&flags.addr, "addr", ":6379", "The TCP address to bind to")
}

func main() {
	flag.Parse()

	if err := run(); err != nil {
		log.Fatalln(err)
	}
}

// bloomToBytes serializes a bloom.BloomFilter to a byte slice. We store
// the capacity and error rate in the first 16 bytes of the byte slice.
func bloomToBytes(filter *bloom.BloomFilter, capacity uint, errorRate float64) ([]byte, error) {
	var blob bytes.Buffer
	binary.Write(&blob, binary.LittleEndian, capacity)
	binary.Write(&blob, binary.LittleEndian, errorRate)
	_, err := filter.WriteTo(&blob)
	if err != nil {
		return nil, err
	}
	return blob.Bytes(), nil
}

// bloomFromBytes deserializes a bloom.BloomFilter from a byte slice including
// the capacity and error rate.
func bloomFromBytes(blob []byte) (*bloom.BloomFilter, uint, float64, error) {
	var capacity uint
	var errorRate float64
	reader := bytes.NewReader(blob)
	binary.Read(reader, binary.LittleEndian, &capacity)
	binary.Read(reader, binary.LittleEndian, &errorRate)
	filter := bloom.NewWithEstimates(capacity, errorRate)
	_, err := filter.ReadFrom(reader)
	if err != nil {
		return nil, 0, 0, err
	}
	return filter, capacity, errorRate, nil
}

// A RedisServer is a wrapper around a badger.DB that implements the
// commands that are supported by the Redy Redis server.
type RedisServer struct {
	db *badger.DB
}

// -------- BLOOM FILTERS --------

func (r *RedisServer) BfInsert(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() < 1 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}

	var (
		filter    *bloom.BloomFilter
		capacity  uint64
		errorRate float64
		err       error
	)
	switch c.ArgN() {
	case 1:
		capacity = 1_000_000
		errorRate = 0.01
	case 2:
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	case 3: // bf.add key CAPACITY <capacity>
		if c.Arg(1).String() != "capacity" && c.Arg(1).String() != "CAPACITY" {
			w.AppendError(redeo.WrongNumberOfArgs(c.Name))
			return
		}
		capacity, err = strconv.ParseUint(c.Arg(2).String(), 10, 32)
		if err != nil {
			w.AppendError(err.Error())
			return
		}
	case 4:
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	case 6:
		fallthrough // adds 'ITEMS' but no actual items
	case 5: // bf.add key CAPACITY <capacity> ERROR <error>
		capacity, err = strconv.ParseUint(c.Arg(2).String(), 10, 32)
		if c.Arg(1).String() != "capacity" && c.Arg(1).String() != "CAPACITY" {
			w.AppendError(redeo.WrongNumberOfArgs(c.Name))
			return
		}
		capacity, err = strconv.ParseUint(c.Arg(2).String(), 10, 32)
		if err != nil {
			w.AppendError(err.Error())
			return
		}
		if c.Arg(3).String() != "error" && c.Arg(3).String() != "ERROR" {
			w.AppendError(redeo.WrongNumberOfArgs(c.Name))
			return
		}
		if err != nil {
			w.AppendError(err.Error())
			return
		}
		errorRate, err = strconv.ParseFloat(c.Arg(4).String(), 32)
		if err != nil {
			w.AppendError(err.Error())
			return
		}
	default:
		w.AppendError(fmt.Sprintf("unsupported number of arguments: %v", c.Args))
	}

	log.Infof("creating bloom filter with key %s capacity %d and error rate %f", c.Arg(0).String(), capacity, errorRate)
	filter = bloom.NewWithEstimates(uint(capacity), errorRate)
	blob, err := bloomToBytes(filter, uint(capacity), errorRate)
	if err != nil {
		w.AppendError(err.Error())
		return
	}

	err = InsertExclusive(r.db, c.Arg(0).Bytes(), blob)
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	w.AppendArrayLen(1)
	w.AppendInt(1)
}

func (r *RedisServer) BfAdd(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() != 2 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	var added bool
	err := UpdateFunc(r.db, c.Arg(0).Bytes(), func(value []byte) ([]byte, error) {
		filter, capacity, errRate, err := bloomFromBytes(value)
		if err != nil {
			return nil, err
		}
		added = filter.TestOrAdd(c.Arg(1).Bytes())
		result, err := bloomToBytes(filter, capacity, errRate)
		if err != nil {
			return nil, err
		}
		return result, nil
	})
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	if added {
		w.AppendInt(1)
	} else {
		w.AppendInt(0)
	}
}

func (r *RedisServer) BfCard(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() != 1 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	value, err := Get(r.db, c.Arg(0).Bytes())
	filter, _, _, err := bloomFromBytes(value)
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	w.AppendInt(int64(filter.ApproximatedSize()))
}

func (r *RedisServer) BfExists(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() != 2 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	value, err := Get(r.db, c.Arg(0).Bytes())
	if err != nil {
		w.AppendError(err.Error())
		return
	}

	filter, _, _, err := bloomFromBytes(value)
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	// Redis returns 1 if the key is in the filter, 0 otherwise
	if filter.Test(c.Arg(1).Bytes()) {
		w.AppendInt(1)
		return
	}
	w.AppendInt(0)
}

// -------- CUCKOO FILTERS --------

func (r *RedisServer) CfInsert(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() < 1 || c.ArgN() > 3 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}

	var (
		filter   *cuckoo.Filter
		capacity uint64
		err      error
	)
	switch c.ArgN() {
	case 1:
		capacity = 1_000_000
	case 2:
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	case 3: // cf.add key CAPACITY <capacity>
		if c.Arg(1).String() != "capacity" && c.Arg(1).String() != "CAPACITY" {
			w.AppendError(redeo.WrongNumberOfArgs(c.Name))
			return
		}
		capacity, err = strconv.ParseUint(c.Arg(2).String(), 10, 32)
		if err != nil {
			w.AppendError(err.Error())
			return
		}
	}

	filter = cuckoo.NewFilter(uint(capacity))
	err = InsertExclusive(r.db, c.Arg(0).Bytes(), filter.Encode())
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	w.AppendOK()
}

func (r *RedisServer) CfCard(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() != 1 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	value, err := Get(r.db, c.Arg(0).Bytes())
	filter, err := cuckoo.Decode(value)
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	w.AppendInt(int64(filter.Count()))
}

func (r *RedisServer) CfAdd(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() != 2 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	var added bool
	err := UpdateFunc(r.db, c.Arg(0).Bytes(), func(value []byte) ([]byte, error) {
		filter, err := cuckoo.Decode(value)
		if err != nil {
			return nil, err
		}
		added = filter.InsertUnique(c.Arg(1).Bytes())
		return filter.Encode(), nil
	})
	if err != nil {
		w.AppendError(err.Error())
		return
	}

	if added {
		w.AppendInt(1)
	} else {
		w.AppendInt(0)
	}
}

func (r *RedisServer) CfExists(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() != 2 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	value, err := Get(r.db, c.Arg(0).Bytes())
	if err != nil {
		w.AppendError(err.Error())
		return
	}

	filter, err := cuckoo.Decode(value)
	if err != nil {
		w.AppendError(err.Error())
		return
	}

	// Redis returns 1 if the key is in the filter, 0 otherwise
	if filter.Lookup(c.Arg(1).Bytes()) {
		w.AppendInt(1)
		return
	}
	w.AppendInt(0)
}

func run() error {
	db, err := badger.Open(badger.DefaultOptions("./redy-badger.db"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	redisSrv := &RedisServer{db: db}

	srv := redeo.NewServer(nil)
	srv.Handle("ping", redeo.Ping())
	srv.Handle("echo", redeo.Echo())
	srv.Handle("info", redeo.Info(srv))

	srv.HandleFunc("bf.insert", redisSrv.BfInsert)
	srv.HandleFunc("bf.add", redisSrv.BfAdd)
	srv.HandleFunc("bf.card", redisSrv.BfCard)
	srv.HandleFunc("bf.exists", redisSrv.BfExists)

	srv.HandleFunc("cf.insert", redisSrv.CfInsert)
	srv.HandleFunc("cf.add", redisSrv.CfAdd)
	srv.HandleFunc("cf.card", redisSrv.CfCard)
	srv.HandleFunc("cf.exists", redisSrv.CfExists)

	srv.HandleFunc("del", func(w resp.ResponseWriter, c *resp.Command) {
		if c.ArgN() < 1 {
			w.AppendError(redeo.WrongNumberOfArgs(c.Name))
			return
		}
		var keys [][]byte
		for _, arg := range c.Args {
			keys = append(keys, arg.Bytes())
		}
		err := DeleteBulk(db, keys)
		if err != nil {
			w.AppendError(err.Error())
			return
		}
		w.AppendInt(int64(len(keys)))
	})

	srv.HandleFunc("set", func(w resp.ResponseWriter, c *resp.Command) {
		if c.ArgN() != 2 {
			w.AppendError(redeo.WrongNumberOfArgs(c.Name))
			return
		}
		err := Insert(db, c.Arg(0).Bytes(), c.Arg(1).Bytes())
		if err != nil {
			w.AppendError(err.Error())
			return
		}
		w.AppendOK()
	})

	srv.HandleFunc("mset", func(w resp.ResponseWriter, c *resp.Command) {
		if c.ArgN() < 2 || c.ArgN()%2 != 0 {
			w.AppendError(redeo.WrongNumberOfArgs(c.Name))
			return
		}
		entries := make([][]byte, c.ArgN())
		for i, arg := range c.Args {
			entries[i] = arg.Bytes()
		}
		err := InsertBulk(db, entries)
		if err != nil {
			w.AppendError(err.Error())
			return
		}
		w.AppendOK()
	})

	srv.HandleFunc("get", func(w resp.ResponseWriter, c *resp.Command) {
		if c.ArgN() != 1 {
			w.AppendError(redeo.WrongNumberOfArgs(c.Name))
			return
		}
		key := c.Arg(0).Bytes()
		val, err := Get(db, key)
		if err == nil {
			w.AppendBulk(val)
			return
		}
		w.AppendNil()
	})

	srv.HandleFunc("mget", func(w resp.ResponseWriter, c *resp.Command) {
		if c.ArgN() < 2 {
			w.AppendError(redeo.WrongNumberOfArgs(c.Name))
			return
		}
		keys := make([][]byte, c.ArgN())
		for i, arg := range c.Args {
			keys[i] = arg.Bytes()
		}

		values, err := GetList(db, keys)
		if err != nil {
			w.AppendError(err.Error())
			return
		}

		w.AppendArrayLen(len(values))
		for _, value := range values {
			w.AppendBulk(value)
		}
	})

	srv.HandleFunc("keys", func(w resp.ResponseWriter, c *resp.Command) {
		if c.ArgN() > 1 {
			w.AppendError(redeo.WrongNumberOfArgs(c.Name))
			return
		}

		var (
			err  error
			keys [][]byte
		)

		switch c.ArgN() {
		case 0:
			// ALL the keys in the database
			err = WalkKeys(db, func(key []byte) error {
				keys = append(keys, key)
				return nil
			})

		case 1:
			// Keys matching the PREFIX (not any old pattern)
			err = WalkKeysPrefix(db, c.Arg(0).Bytes(), func(key []byte) error {
				keys = append(keys, key)
				return nil
			})
		}

		if err != nil {
			w.AppendError(err.Error())
			return
		}
		w.AppendArrayLen(len(keys))
		for _, key := range keys {
			w.AppendBulk(key)
		}
	})

	lis, err := net.Listen("tcp", flags.addr)
	if err != nil {
		return err
	}
	defer lis.Close()

	log.Printf("waiting for connections on %s", lis.Addr().String())
	return srv.Serve(lis)
}
