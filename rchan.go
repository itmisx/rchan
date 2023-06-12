package rchan

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// channel loker
	defaultChannelLockerName = "system:redis-channel:lock"
	// channel list name
	defaultChannelName = "system:redis-channel:list"
)

type rchan struct {
	ChannelName       string
	ChannelLockerName string
	rdb               redis.Cmdable
	MaxLen            int64
	timeout           time.Duration
}

func New(options ...option) *rchan {
	rc := &rchan{}
	for _, o := range options {
		o(rc)
	}
	if rc.ChannelName == "" {
		rc.ChannelName = defaultChannelName
	}
	if rc.ChannelLockerName == "" {
		rc.ChannelLockerName = defaultChannelLockerName
	}
	if rc.timeout == 0 {
		rc.timeout = time.Second * 3
	}
	if rc.rdb == nil {
		panic("redis server can not be empty")
	}
	return rc
}

// Push 向通道中写入数据
func (rc *rchan) Push(val interface{}) (int64, error) {
	rc.lock()
	defer rc.unlock()

	ctx, cancel := context.WithTimeout(context.Background(), rc.timeout)
	defer cancel()
	l, err := rc.rdb.LLen(ctx, rc.ChannelName).Result()
	if err != nil {
		return 0, err
	}
	if rc.MaxLen > 0 && l >= rc.MaxLen {
		return 0, errors.New("channel is full")
	}
	return rc.rdb.LPush(ctx, rc.ChannelName, val).Result()
}

// Pop 从通道中读取数据
func (rc *rchan) Pop() (string, error) {
	rc.lock()
	defer rc.unlock()
	ctx, cancel := context.WithTimeout(context.Background(), rc.timeout)
	defer cancel()
	val, err := rc.rdb.RPop(ctx, rc.ChannelName).Result()
	if val == "" {
		time.Sleep(time.Millisecond * 100)
	}
	return val, err
}

// Len 获取通道的长度
func (rc *rchan) Len() (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), rc.timeout)
	defer cancel()
	return rc.rdb.LLen(ctx, rc.ChannelName).Result()
}

// Clear 删除全部的通道数据
func (rc *rchan) Del() (int64, error) {
	rc.lock()
	defer rc.unlock()
	ctx, cancel := context.WithTimeout(context.Background(), rc.timeout)
	defer cancel()
	return rc.rdb.Del(ctx, rc.ChannelName).Result()
}

// lock 加锁
func (rc *rchan) lock() {
	for {
		success, _ := rc.rdb.SetNX(context.Background(), rc.ChannelLockerName, 1, time.Second*60).Result()
		if success {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}
}

// unlock 释放锁
func (rc *rchan) unlock() {
	rc.rdb.Del(context.Background(), rc.ChannelLockerName)
}

type option func(*rchan)

// WithChannelKeyName channel名称
func WithChannelName(channelName string) option {
	return func(rc *rchan) {
		rc.ChannelName = channelName
	}
}

// WithChannelLokerName channel loker名称
func WithChannelLokerName(channelLockerName string) option {
	return func(rc *rchan) {
		rc.ChannelLockerName = channelLockerName
	}
}

// WithTimeout 超时时间
func WithTimeout(timeout time.Duration) option {
	return func(rc *rchan) {
		rc.timeout = timeout
	}
}

// WithMaxLen 通道最大长度
func WithMaxLen(len int64) option {
	return func(rc *rchan) {
		rc.MaxLen = len
	}
}

// WithRedisConfig redis-server配置
func WithRedisConfig(addr, password string, db int) option {
	return func(rc *rchan) {
		for {
			rc.rdb = redis.NewClient(&redis.Options{
				Addr:     addr,
				Password: password,
				DB:       db,
			})
			res, err := rc.rdb.Ping(context.Background()).Result()
			if strings.ToLower(res) != "pong" || err != nil {
				log.Println("redis connection failed,retry...")
				time.Sleep(time.Second * 5)
			} else {
				break
			}
		}
	}
}

// WithRedisClusterConfig redis-cluster配置
func WithRedisClusterConfig(addrs []string, password string) option {
	return func(rc *rchan) {
		for {
			rc.rdb = redis.NewClusterClient(&redis.ClusterOptions{
				Addrs:    addrs,
				Password: password,
			})
			res, err := rc.rdb.Ping(context.Background()).Result()
			if strings.ToLower(res) != "pong" || err != nil {
				log.Println("redis connection failed,retry...")
				time.Sleep(time.Second * 5)
			} else {
				break
			}
		}
	}
}
