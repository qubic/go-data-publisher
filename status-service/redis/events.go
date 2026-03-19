package redis

import (
	"context"
	"fmt"
	"strconv"

	"github.com/qubic/go-data-publisher/status-service/domain"
	"github.com/redis/go-redis/v9"
)

type EventsRedisClient struct {
	rdb     *redis.Client
	keyName string
}

type EventsRedisClientCfg struct {
	MasterName        string
	SentinelAddresses []string
	SentinelPassword  string
	Password          string
	Db                int
	KeyName           string
}

func NewEventsClient(cfg EventsRedisClientCfg) *EventsRedisClient {
	rdb := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:       cfg.MasterName,
		SentinelAddrs:    cfg.SentinelAddresses,
		SentinelPassword: cfg.SentinelPassword,
		Password:         cfg.Password,
		DB:               cfg.Db,
	})

	return &EventsRedisClient{
		rdb:     rdb,
		keyName: cfg.KeyName,
	}
}

func (er *EventsRedisClient) GetEventsLastIngestedTickStatus(ctx context.Context) (consumedEventLogTick domain.RedisEventsLastIngestedTickStatus, exists bool, err error) {
	values, err := er.rdb.HGetAll(ctx, er.keyName).Result()
	if err != nil {
		return domain.RedisEventsLastIngestedTickStatus{}, false, fmt.Errorf("getting events last ingested tick status: %w", err)
	}

	if len(values) == 0 {
		return domain.RedisEventsLastIngestedTickStatus{}, false, nil
	}

	tickNumber, err := strconv.ParseUint(values["tickNumber"], 10, 32)
	if err != nil {
		return domain.RedisEventsLastIngestedTickStatus{}, false, fmt.Errorf("parsing tick number: %w", err)
	}

	eventCount, err := strconv.ParseUint(values["count"], 10, 32)
	if err != nil {
		return domain.RedisEventsLastIngestedTickStatus{}, false, fmt.Errorf("parsing event count: %w", err)
	}

	return domain.RedisEventsLastIngestedTickStatus{
		TickNumber: uint32(tickNumber),
		EventCount: uint32(eventCount),
	}, true, nil
}

func (er *EventsRedisClient) Close() error {
	return er.rdb.Close()
}
