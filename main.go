package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"net"
	"strconv"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/bsm/redeo"
	"github.com/bsm/redeo/resp"
	"github.com/dgraph-io/badger/v4"
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

func run() error {
	db, err := badger.Open(badger.DefaultOptions("./redy-badger.db"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	srv := redeo.NewServer(nil)
	srv.Handle("ping", redeo.Ping())
	srv.Handle("echo", redeo.Echo())
	srv.Handle("info", redeo.Info(srv))

	srv.HandleFunc("bf.insert", func(w resp.ResponseWriter, c *resp.Command) {
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
			capacity, err = strconv.ParseUint(c.Arg(2).String(), 10, 32)
			if err != nil {
				w.AppendError(err.Error())
				return
			}
		case 4:
			w.AppendError(redeo.WrongNumberOfArgs(c.Name))
			return
		case 5: // bf.add key CAPACITY <capacity> ERROR <error>
			capacity, err = strconv.ParseUint(c.Arg(2).String(), 10, 32)
			if err != nil {
				w.AppendError(err.Error())
				return
			}
			errorRate, err = strconv.ParseFloat(c.Arg(4).String(), 32)
			if err != nil {
				w.AppendError(err.Error())
				return
			}
		}

		filter = bloom.NewWithEstimates(uint(capacity), errorRate)
		blob, err := bloomToBytes(filter, 0, 0.01)
		if err != nil {
			w.AppendError(err.Error())
			return
		}

		err = InsertExclusive(db, c.Arg(0).Bytes(), blob)
		if err != nil {
			w.AppendError(err.Error())
			return
		}
		w.AppendOK()
	})

	srv.HandleFunc("bf.add", func(w resp.ResponseWriter, c *resp.Command) {
		if c.ArgN() != 2 {
			w.AppendError(redeo.WrongNumberOfArgs(c.Name))
			return
		}
		err := UpdateFunc(db, c.Arg(0).Bytes(), func(value []byte) ([]byte, error) {
			filter, capacity, errRate, err := bloomFromBytes(value)
			if err != nil {
				return nil, err
			}
			filter = filter.Add(c.Arg(1).Bytes())
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
		w.AppendOK()
	})

	srv.HandleFunc("bf.card", func(w resp.ResponseWriter, c *resp.Command) {
		if c.ArgN() != 1 {
			w.AppendError(redeo.WrongNumberOfArgs(c.Name))
			return
		}
		value, err := Get(db, c.Arg(0).Bytes())
		filter, _, _, err := bloomFromBytes(value)
		if err != nil {
			w.AppendError(err.Error())
			return
		}
		w.AppendInt(int64(filter.ApproximatedSize()))
	})

	srv.HandleFunc("bf.exists", func(w resp.ResponseWriter, c *resp.Command) {
		if c.ArgN() != 2 {
			w.AppendError(redeo.WrongNumberOfArgs(c.Name))
			return
		}
		value, err := Get(db, c.Arg(0).Bytes())
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
	})

	srv.HandleFunc("del", func(w resp.ResponseWriter, c *resp.Command) {
		if c.ArgN() != 1 {
			w.AppendError(redeo.WrongNumberOfArgs(c.Name))
			return
		}
		err := Delete(db, c.Arg(0))
		if err != nil {
			w.AppendError(err.Error())
			return
		}
		w.AppendOK()
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
