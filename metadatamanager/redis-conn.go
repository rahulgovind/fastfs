package metadatamanager

import (
	"github.com/go-redis/redis"
	log "github.com/sirupsen/logrus"
	"time"
)

type RedisConn struct {
	client *redis.Client
}

func NewRedisConn(addr string) *RedisConn {
	conn := new(RedisConn)
	conn.client = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: "",
		DB:       0,
		DialTimeout: 5 * time.Second,
		PoolSize:    300,
	})
	return conn
}

func (rc *RedisConn) Get(key string) (string, bool) {
	val, err := rc.client.Get(key).Result()
	if err != nil {
		if err == redis.Nil {
			return "", false
		}
		log.Fatal(err)
	}

	return val, true
}

func (rc *RedisConn) Set(key string, value string) {
	err := rc.client.Set(key, value, time.Hour).Err()
	if err != nil {
		log.Fatalf("%v %s %s", err, key, value)
	}
}

func (rc *RedisConn) MGet(keys []string) (values []string, oks []bool) {
	if len(keys) == 0 {
		return
	}

	vals, err := rc.client.MGet(keys...).Result()
	if err != nil {
		log.Fatal(err)
	}
	for _, val := range vals {
		if val == redis.Nil {
			values = append(values, "")
			oks = append(oks, false)
		} else {
			values = append(values, val.(string))
			oks = append(oks, true)
		}
	}
	return
}

func (rc *RedisConn) MSet(keys []string, values []string) {
	var pairs []string
	if len(keys) != len(values) {
		log.Fatal("(MSET) Number of keys != Number of values")
	}

	if len(keys) == 0 {
		return
	}

	for i := range keys {
		pairs = append(pairs, keys[i], values[i])
	}
	s, err := rc.client.MSet(pairs).Result()

	if err != nil {
		log.Fatal(s, err)
	}
}

func (rc *RedisConn) Delete(key string) {
	rc.client.Del(key).Result()
}

func (rc *RedisConn) ListGet(key string) ([]string, bool) {
	key = "__" + key
	val, err := rc.client.SMembers(key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, false
		}
		log.Fatal(err)
	}

	if len(val) == 0 {
		return nil, false
	}

	return val, true
}

func (rc *RedisConn) ListDelete(key string) {
	key = "__" + key
	rc.client.SRem(key).Result()
}

func (rc *RedisConn) ListAdd(key string, values ...string) {
	key = "__" + key
	rc.client.SAdd(key, values).Result()
}

func (rc *RedisConn) Flush() {
	rc.client.FlushAll().Val()
}
