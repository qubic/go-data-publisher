package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/qubic/go-data-publisher/status-service/domain"
	"github.com/redis/go-redis/v9"
)

type EventsRedisClient struct {
	rdb       *redis.Client
	keyPrefix string
}

type EventsRedisClientCfg struct {
	MasterName        string
	SentinelAddresses []string
	Password          string
	Db                int
	KeyPrefix         string
}

func NewEventsClient(cfg EventsRedisClientCfg) *EventsRedisClient {
	rdb := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    cfg.MasterName,
		SentinelAddrs: cfg.SentinelAddresses,
		Password:      cfg.Password,
		DB:            cfg.Db,
	})

	return &EventsRedisClient{
		rdb:       rdb,
		keyPrefix: cfg.KeyPrefix,
	}
}

func (er *EventsRedisClient) GetConsumedEventLogTick(ctx context.Context, tickNumber uint32) (consumedEventLogTick domain.RedisConsumedEventLogTick, exists bool, err error) {
	key := fmt.Sprintf("%s%d", er.keyPrefix, tickNumber)
	values, err := er.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return domain.RedisConsumedEventLogTick{}, false, fmt.Errorf("getting consumed event log tick hash from redis: %w", err)
	}

	if len(values) == 0 {
		return domain.RedisConsumedEventLogTick{}, false, nil
	}

	total, err := strconv.Atoi(values["total"])
	if err != nil {
		return domain.RedisConsumedEventLogTick{}, false, fmt.Errorf("parsing total: %w", err)
	}

	stored, err := strconv.Atoi(values["stored"])
	if err != nil {
		return domain.RedisConsumedEventLogTick{}, false, fmt.Errorf("parsing stored: %w", err)
	}

	dropped, err := strconv.Atoi(values["dropped"])
	if err != nil {
		return domain.RedisConsumedEventLogTick{}, false, fmt.Errorf("parsing dropped: %w", err)
	}

	timestampMs, err := strconv.ParseInt(values["timestamp"], 10, 64)
	if err != nil {
		return domain.RedisConsumedEventLogTick{}, false, fmt.Errorf("parsing timestamp: %w", err)
	}

	return domain.RedisConsumedEventLogTick{
		Total:     total,
		Stored:    stored,
		Dropped:   dropped,
		Timestamp: time.UnixMilli(timestampMs),
	}, true, nil
}

func (er *EventsRedisClient) Close() error {
	return er.rdb.Close()
}
