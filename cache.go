package cache

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/klauspost/compress/s2"
	"github.com/vmihailenco/bufpool"
	"github.com/vmihailenco/msgpack/v5"
	"golang.org/x/sync/singleflight"
)

const (
	compressionThreshold = 64
	timeLen              = 4
)

const (
	noCompression = 0x0
	s2Compression = 0x1
)

var (
	ErrCacheMiss          = errors.New("cache: key is missing")
	errRedisLocalCacheNil = errors.New("cache: both Redis and LocalCache are nil")
)

type rediser interface {
	Set(ctx context.Context, key string, value interface{}, ttl time.Duration) *redis.StatusCmd
	SetXX(ctx context.Context, key string, value interface{}, ttl time.Duration) *redis.BoolCmd
	SetNX(ctx context.Context, key string, value interface{}, ttl time.Duration) *redis.BoolCmd

	Get(ctx context.Context, key string) *redis.StringCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	Pipeline() redis.Pipeliner
}

type Item struct {
	Ctx context.Context

	Key   string
	Value interface{}

	// TTL is the cache expiration time.
	// Default TTL is 1 hour.
	TTL time.Duration

	// Do returns value to be cached.
	Do func(*Item) (interface{}, error)

	// SetXX only sets the key if it already exists.
	SetXX bool

	// SetNX only sets the key if it does not already exist.
	SetNX bool

	// SkipLocalCache skips local cache as if it is not set.
	SkipLocalCache bool
}

func (item *Item) Context() context.Context {
	if item.Ctx == nil {
		return context.Background()
	}
	return item.Ctx
}

func (item *Item) value() (interface{}, error) {
	if item.Do != nil {
		return item.Do(item)
	}
	if item.Value != nil {
		return item.Value, nil
	}
	return nil, nil
}

func (item *Item) ttl() time.Duration {
	if item.TTL < 0 {
		return 0
	}
	if item.TTL != 0 && item.TTL < time.Second {
		log.Printf("too short TTL for key=%q: %s", item.Key, item.TTL)
		return time.Hour
	}
	return item.TTL
}

type ValueSlice interface {
	Len() int

	// Get return the value at index which will be set into cache.
	Get(index int) interface{}
	// GetPtr return the pointer of non-nil value at index
	// which will be unmarshalled from value.
	GetPtr(index int) interface{}
}

type MItem struct {
	Ctx context.Context

	Keys []string
	// ValueSlice is value slice whose length is equal to length of keys.
	ValueSlice ValueSlice

	// TTL is the cache expiration time.
	// Default TTL is 1 hour.
	TTL time.Duration
}

func (mItem *MItem) Context() context.Context {
	if mItem.Ctx == nil {
		return context.Background()
	}
	return mItem.Ctx
}

func (mItem *MItem) valueSlice() (ValueSlice, error) {
	if mItem.ValueSlice != nil {
		return mItem.ValueSlice, nil
	}
	return nil, nil
}

func (mItem *MItem) ttl() time.Duration {
	if mItem.TTL < 0 {
		return 0
	}
	if mItem.TTL != 0 && mItem.TTL < time.Second {
		log.Printf("too short TTL for keys=%q: %s", mItem.Keys, mItem.TTL)
		return time.Hour
	}
	return mItem.TTL
}

//------------------------------------------------------------------------------

type Options struct {
	Redis        rediser
	LocalCache   LocalCache
	StatsEnabled bool
}

type Cache struct {
	opt *Options

	group   singleflight.Group
	bufpool bufpool.Pool

	hits   uint64
	misses uint64
}

func New(opt *Options) *Cache {
	return &Cache{
		opt: opt,
	}
}

// Set caches the item.
func (cd *Cache) Set(item *Item) error {
	_, _, err := cd.set(item)
	return err
}

func (cd *Cache) set(item *Item) ([]byte, bool, error) {
	value, err := item.value()
	if err != nil {
		return nil, false, err
	}

	b, err := cd.Marshal(value)
	if err != nil {
		return nil, false, err
	}

	if cd.opt.LocalCache != nil {
		cd.opt.LocalCache.Set(item.Key, b)
	}

	if cd.opt.Redis == nil {
		if cd.opt.LocalCache == nil {
			return b, true, errRedisLocalCacheNil
		}
		return b, true, nil
	}

	if item.SetXX {
		return b, true, cd.opt.Redis.SetXX(item.Context(), item.Key, b, item.ttl()).Err()
	}

	if item.SetNX {
		return b, true, cd.opt.Redis.SetNX(item.Context(), item.Key, b, item.ttl()).Err()
	}

	return b, true, cd.opt.Redis.Set(item.Context(), item.Key, b, item.ttl()).Err()
}

// Exists reports whether value for the given key exists.
func (cd *Cache) Exists(ctx context.Context, key string) bool {
	return cd.Get(ctx, key, nil) == nil
}

// Get gets the value for the given key.
func (cd *Cache) Get(ctx context.Context, key string, value interface{}) error {
	return cd.get(ctx, key, value, false)
}

// Get gets the value for the given key skipping local cache.
func (cd *Cache) GetSkippingLocalCache(
	ctx context.Context, key string, value interface{},
) error {
	return cd.get(ctx, key, value, true)
}

func (cd *Cache) get(
	ctx context.Context,
	key string,
	value interface{},
	skipLocalCache bool,
) error {
	b, err := cd.getBytes(ctx, key, skipLocalCache)
	if err != nil {
		return err
	}
	return cd.Unmarshal(b, value)
}

func (cd *Cache) getBytes(ctx context.Context, key string, skipLocalCache bool) ([]byte, error) {
	if !skipLocalCache && cd.opt.LocalCache != nil {
		b, ok := cd.opt.LocalCache.Get(key)
		if ok {
			return b, nil
		}
	}

	if cd.opt.Redis == nil {
		if cd.opt.LocalCache == nil {
			return nil, errRedisLocalCacheNil
		}
		return nil, ErrCacheMiss
	}

	b, err := cd.opt.Redis.Get(ctx, key).Bytes()
	if err != nil {
		if cd.opt.StatsEnabled {
			atomic.AddUint64(&cd.misses, 1)
		}
		if err == redis.Nil {
			return nil, ErrCacheMiss
		}
		return nil, err
	}

	if cd.opt.StatsEnabled {
		atomic.AddUint64(&cd.hits, 1)
	}

	if !skipLocalCache && cd.opt.LocalCache != nil {
		cd.opt.LocalCache.Set(key, b)
	}
	return b, nil
}

// Once gets the item.Value for the given item.Key from the cache or
// executes, caches, and returns the results of the given item.Func,
// making sure that only one execution is in-flight for a given item.Key
// at a time. If a duplicate comes in, the duplicate caller waits for the
// original to complete and receives the same results.
func (cd *Cache) Once(item *Item) error {
	b, cached, err := cd.getSetItemBytesOnce(item)
	if err != nil {
		return err
	}

	if item.Value == nil || len(b) == 0 {
		return nil
	}

	if err := cd.Unmarshal(b, item.Value); err != nil {
		if cached {
			_ = cd.Delete(item.Context(), item.Key)
			return cd.Once(item)
		}
		return err
	}

	return nil
}

func (cd *Cache) getSetItemBytesOnce(item *Item) (b []byte, cached bool, err error) {
	if cd.opt.LocalCache != nil {
		b, ok := cd.opt.LocalCache.Get(item.Key)
		if ok {
			return b, true, nil
		}
	}

	v, err, _ := cd.group.Do(item.Key, func() (interface{}, error) {
		b, err := cd.getBytes(item.Context(), item.Key, item.SkipLocalCache)
		if err == nil {
			cached = true
			return b, nil
		}

		b, ok, err := cd.set(item)
		if ok {
			return b, nil
		}
		return nil, err
	})
	if err != nil {
		return nil, false, err
	}
	return v.([]byte), cached, nil
}

// MSet caches the mItem.
func (cd *Cache) MSet(mItem *MItem) error {
	return cd.mSet(mItem)
}

func (cd *Cache) mSet(mItem *MItem) error {
	valueSlice, err := mItem.valueSlice()
	if err != nil {
		return err
	}
	if len(mItem.Keys) != valueSlice.Len() {
		return errors.New("length of keys and values is not equal")
	}

	var pipeline redis.Pipeliner
	if cd.opt.Redis != nil {
		pipeline = cd.opt.Redis.Pipeline()
	}

	ctx, ttl := mItem.Context(), mItem.ttl()
	for i, key := range mItem.Keys {
		value := valueSlice.Get(i)
		bytes, err := cd.Marshal(value)
		if err != nil {
			return err
		}

		if cd.opt.LocalCache != nil {
			cd.opt.LocalCache.Set(key, bytes)
		}

		if pipeline != nil {
			pipeline.Set(ctx, key, bytes, ttl)
		}
	}

	if pipeline != nil {
		_, err = pipeline.Exec(ctx)
	}

	return err
}

// MGet gets the value for the given key.
func (cd *Cache) MGet(ctx context.Context, keys []string, valueSlice ValueSlice) (map[int]string, error) {
	return cd.mGet(ctx, keys, valueSlice, false)
}

// MGetSkippingLocalCache gets the value for the given key skipping local cache.
func (cd *Cache) MGetSkippingLocalCache(
	ctx context.Context, keys []string, valueSlice ValueSlice,
) (map[int]string, error) {
	return cd.mGet(ctx, keys, valueSlice, true)
}

func (cd *Cache) mGet(
	ctx context.Context,
	keys []string,
	valueSlice ValueSlice,
	skipLocalCache bool,
) (map[int]string, error) {
	byteSlices, missingIndexMap, err := cd.mGetBytes(ctx, keys, skipLocalCache)
	if err != nil {
		return nil, err
	}

	for i, byteSlice := range byteSlices {
		// Only unmarshal when key isn't missing
		if _, exists := missingIndexMap[i]; !exists {
			value := valueSlice.GetPtr(i)
			if err := cd.Unmarshal(byteSlice, value); err != nil {
				return nil, err
			}
		}
	}

	return missingIndexMap, nil
}

func (cd *Cache) mGetBytes(ctx context.Context, keys []string, skipLocalCache bool) ([][]byte, map[int]string, error) {
	byteSlices := make([][]byte, len(keys))

	// Get bytes from local cache
	var absentKeys []string
	var absentIndices []int
	if !skipLocalCache && cd.opt.LocalCache != nil {
		for i, key := range keys {
			byteSlice, ok := cd.opt.LocalCache.Get(key)
			if ok {
				byteSlices[i] = byteSlice
			} else {
				absentKeys = append(absentKeys, key)
				absentIndices = append(absentIndices, i)
			}
		}

		if len(absentKeys) == 0 {
			return byteSlices, nil, nil
		}
	} else {
		absentKeys = keys
		absentIndices = make([]int, len(keys))
		for i := range absentIndices {
			absentIndices[i] = i
		}
	}

	if cd.opt.Redis == nil {
		if cd.opt.LocalCache == nil {
			return nil, nil, errRedisLocalCacheNil
		}
		return nil, nil, ErrCacheMiss
	}

	// Get absent bytes from redis
	pipeline := cd.opt.Redis.Pipeline()
	cmds := make([]*redis.StringCmd, len(absentKeys))
	for i, key := range absentKeys {
		cmds[i] = pipeline.Get(ctx, key)
	}
	_, err := pipeline.Exec(ctx)
	if err != nil && err != redis.Nil {
		return nil, nil, err
	}

	// Fill absent bytes and record missing key indices
	missingIndexMap := make(map[int]string)
	for i, index := range absentIndices {
		byteSlices[index], err = cmds[i].Bytes()
		if err == redis.Nil {
			missingIndexMap[index] = keys[index]
			continue
		}
		if err != nil {
			if cd.opt.StatsEnabled {
				atomic.AddUint64(&cd.hits, uint64(i-len(missingIndexMap)))
				atomic.AddUint64(&cd.misses, uint64(len(missingIndexMap)+1))
			}

			return nil, nil, err
		}

		if !skipLocalCache && cd.opt.LocalCache != nil {
			cd.opt.LocalCache.Set(absentKeys[index], byteSlices[index])
		}
	}

	if cd.opt.StatsEnabled {
		atomic.AddUint64(&cd.hits, uint64(len(absentIndices)-len(missingIndexMap)))
		atomic.AddUint64(&cd.misses, uint64(len(missingIndexMap)))
	}

	return byteSlices, missingIndexMap, nil
}

func (cd *Cache) Delete(ctx context.Context, key string) error {
	if cd.opt.LocalCache != nil {
		cd.opt.LocalCache.Del(key)
	}

	if cd.opt.Redis == nil {
		if cd.opt.LocalCache == nil {
			return errRedisLocalCacheNil
		}
		return nil
	}

	_, err := cd.opt.Redis.Del(ctx, key).Result()
	return err
}

func (cd *Cache) Marshal(value interface{}) ([]byte, error) {
	switch value := value.(type) {
	case nil:
		return nil, nil
	case []byte:
		return value, nil
	case string:
		return []byte(value), nil
	}

	buf := cd.bufpool.Get()
	defer cd.bufpool.Put(buf)

	enc := msgpack.GetEncoder()
	enc.Reset(buf)
	enc.UseCompactInts(true)

	err := enc.Encode(value)

	msgpack.PutEncoder(enc)

	if err != nil {
		return nil, err
	}

	return compress(buf.Bytes()), nil
}

func compress(data []byte) []byte {
	if len(data) < compressionThreshold {
		n := len(data) + 1
		b := make([]byte, n, n+timeLen)
		copy(b, data)
		b[len(b)-1] = noCompression
		return b
	}

	n := s2.MaxEncodedLen(len(data)) + 1
	b := make([]byte, n, n+timeLen)
	b = s2.Encode(b, data)
	b = append(b, s2Compression)
	return b
}

func (cd *Cache) Unmarshal(b []byte, value interface{}) error {
	if len(b) == 0 {
		return nil
	}

	switch value := value.(type) {
	case nil:
		return nil
	case *[]byte:
		clone := make([]byte, len(b))
		copy(clone, b)
		*value = clone
		return nil
	case *string:
		*value = string(b)
		return nil
	}

	switch c := b[len(b)-1]; c {
	case noCompression:
		b = b[:len(b)-1]
	case s2Compression:
		b = b[:len(b)-1]

		n, err := s2.DecodedLen(b)
		if err != nil {
			return err
		}

		buf := bufpool.Get(n)
		defer bufpool.Put(buf)

		b, err = s2.Decode(buf.Bytes(), b)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown compression method: %x", c)
	}

	return msgpack.Unmarshal(b, value)
}

//------------------------------------------------------------------------------

type Stats struct {
	Hits   uint64
	Misses uint64
}

// Stats returns cache statistics.
func (cd *Cache) Stats() *Stats {
	if !cd.opt.StatsEnabled {
		return nil
	}
	return &Stats{
		Hits:   atomic.LoadUint64(&cd.hits),
		Misses: atomic.LoadUint64(&cd.misses),
	}
}
