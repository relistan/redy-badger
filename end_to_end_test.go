package main

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/redis/go-redis/v9"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	RedisPort = 6378 // Don't conflict with the default Redis port
	TestAddress = fmt.Sprintf("localhost:%d", RedisPort)
)

func init() {
	flags.addr = fmt.Sprintf(":%d", RedisPort)
	go run()
}

func TestEndToEnd(t *testing.T) {
	Convey("End to end test", t, func() {
		Reset(CleanUp)

		client := redis.NewClient(&redis.Options{
			Addr: TestAddress,
		})
		defer client.Close()
		ctx := context.Background()

		Convey("Ping", func() {
			err := client.Ping(ctx).Err()
			So(err, ShouldBeNil)
		})

		Convey("Set", func() {
			_, err := client.Set(ctx, "asdf", "asdfasdf", 0).Result()
			So(err, ShouldBeNil)
		})

		Convey("Get", func() {
			_, err := client.Set(ctx, "asdf", "asdfasdf", 0).Result()
			So(err, ShouldBeNil)
			val, err := client.Get(ctx, "asdf").Result()
			So(err, ShouldBeNil)
			So(val, ShouldEqual, "asdfasdf")
		})

		Convey("Del", func() {
			_, err := client.Del(ctx, "asdf").Result()
			So(err, ShouldBeNil)
			_, err = client.Get(ctx, "asdf").Result()
			So(errors.Is(err, redis.Nil), ShouldBeTrue)
		})

		Convey("Bloom filter end-to-end", func() {
			_, err := client.BFInsert(ctx, "bloom", &redis.BFInsertOptions{
				Capacity: 50_000,
				Error:    0.01,
			}).Result()
			So(err, ShouldBeNil)

			// Add some keys to the filter
			_, err = client.BFAdd(ctx, "bloom", "+13164647972").Result()
			So(err, ShouldBeNil)
			_, err = client.BFAdd(ctx, "bloom", "+353873549906").Result()
			So(err, ShouldBeNil)
			_, err = client.BFAdd(ctx, "bloom", "+4401483123445").Result()
			So(err, ShouldBeNil)

			// Check the cardinality
			card, err := client.BFCard(ctx, "bloom").Result()
			So(err, ShouldBeNil)
			So(card, ShouldEqual, 3)

			// Check if the key exists
			exists, err := client.BFExists(ctx, "bloom", "+353873549906").Result()
			So(err, ShouldBeNil)
			So(exists, ShouldEqual, true)
		})

		Convey("Cuckoo filter end-to-end", func() {
			_, err := client.CFInsert(ctx, "cuckoo", &redis.CFInsertOptions{
				Capacity: 50_000,
			}).Result()
			So(err, ShouldBeNil)

			// Add some keys to the filter
			_, err = client.CFAdd(ctx, "cuckoo", "+13164647972").Result()
			So(err, ShouldBeNil)
			_, err = client.CFAdd(ctx, "cuckoo", "+353873549906").Result()
			So(err, ShouldBeNil)
			_, err = client.CFAdd(ctx, "cuckoo", "+4401483123445").Result()
			So(err, ShouldBeNil)

			// Check the cardinality
			card, err := CFCard(ctx, client, "cuckoo").Result()
			So(err, ShouldBeNil)
			So(card, ShouldEqual, 3)

			// Check if the key exists
			exists, err := client.CFExists(ctx, "cuckoo", "+353873549906").Result()
			So(exists, ShouldBeTrue)
			So(err, ShouldBeNil)

			// Delete a key
			deleted, err := client.CFDel(ctx, "cuckoo", "+353873549906").Result()
			So(err, ShouldBeNil)
			So(deleted, ShouldBeTrue)

			// Check the cardinality
			card, err = CFCard(ctx, client, "cuckoo").Result()
			So(err, ShouldBeNil)
			So(card, ShouldEqual, 2)

			// Check if the key exists
			exists, err = client.CFExists(ctx, "cuckoo", "+353873549906").Result()
			So(exists, ShouldBeFalse)
			So(err, ShouldBeNil)
		})
	})
}

// CFCard is a custom Cuckoo Filter function not supported by Redis
func CFCard(ctx context.Context, rdb *redis.Client, key string) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx, "cf.card", key)
	rdb.Process(ctx, cmd)
	return cmd
}

func CleanUp() {
	client := redis.NewClient(&redis.Options{
		Addr: TestAddress,
	})

	ctx := context.Background()

	keys, err := client.Keys(ctx, "").Result()
	So(err, ShouldBeNil)

	for _, key := range keys {
		_, err := client.Del(ctx, key).Result()
		So(err, ShouldBeNil)
	}
}
