package main

import (
	"errors"

	"github.com/dgraph-io/badger/v4"
)

var ErrKeyExists = errors.New("key already exists")

func Delete(db *badger.DB, key []byte) error {
	return db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

func DeleteBulk(db *badger.DB, keys [][]byte) error {
	var errs []error
	return db.Update(func(txn *badger.Txn) error {
		for _, key := range keys {
			if err := txn.Delete(key); err != nil {
				errs = append(errs, err)
			}
		}
		return errors.Join(errs...)
	})
}

func InsertExclusive(db *badger.DB, key []byte, value []byte) error {
	return db.Update(func(txn *badger.Txn) error {
		if existing, _ := txn.Get([]byte(key)); existing != nil {
			return ErrKeyExists
		}
		return txn.Set([]byte(key), []byte(value))
	})
}

func Insert(db *badger.DB, key []byte, value []byte) error {
	return db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), []byte(value))
	})
}

func InsertBulk(db *badger.DB, entries [][]byte) error {
	return db.Update(func(txn *badger.Txn) error {
		var errs []error
		for i := 0; i < len(entries); i += 2 {
			key := entries[i]
			value := entries[i+1]
			errs = append(errs, txn.Set([]byte(key), []byte(value)))
		}
		return errors.Join(errs...)
	})
}

func Get(db *badger.DB, key []byte) ([]byte, error) {
	var value []byte
	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		// Copy the contents of the item to a new byte slice so we can return it
		// without worrying about reuse of the original buffer.
		value, err = item.ValueCopy(nil)
		return err
	})
	if err != nil {
		return nil, err
	}
	return value, nil
}

func GetList(db *badger.DB, keys [][]byte) ([][]byte, error) {
	var values [][]byte
	err := db.View(func(txn *badger.Txn) error {
		for _, key := range keys {
			item, err := txn.Get(key)
			if err != nil {
				return err
			}
			// Copy the contents of the item to a new byte slice so we can return it
			// without worrying about reuse of the original buffer.
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			values = append(values, value)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return values, nil
}

func UpdateFunc(db *badger.DB, key []byte, fn func(value []byte) ([]byte, error)) error {
	return db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		var result []byte
		item.Value(func(val []byte) error {
			result, err = fn(val)
			return err
		})
		return txn.Set(key, result)
	})
}

func WalkKeys(db *badger.DB, fn func(key []byte) error) error {
	var errs []error
	_ = db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			key := make([]byte, len(it.Item().Key()))
			copy(key, it.Item().Key())
			errs = append(errs, fn(key))
		}
		return nil
	})
	return errors.Join(errs...)
}

func WalkKeysPrefix(db *badger.DB, prefix []byte, fn func(key []byte) error) error {
	var errs []error
	_ = db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := make([]byte, len(it.Item().Key()))
			copy(key, it.Item().Key())
			errs = append(errs, fn(key))
		}
		return nil
	})
	return errors.Join(errs...)
}
