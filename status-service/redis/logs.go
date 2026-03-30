package redis

import (
	"context"
	"fmt"
	"strconv"

	"github.com/qubic/go-data-publisher/status-service/domain"
	"github.com/redis/go-redis/v9"
)

type LogsRedisClient struct {
	rdb                  *redis.Client
	logLastTickStatusKey string
}

type LogsRedisClientCfg struct {
	MasterName           string
	SentinelAddresses    []string
	SentinelPassword     string
	Password             string
	Db                   int
	LogLastTickStatusKey string
}

func NewLogsClient(cfg LogsRedisClientCfg) *LogsRedisClient {
	rdb := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:       cfg.MasterName,
		SentinelAddrs:    cfg.SentinelAddresses,
		SentinelPassword: cfg.SentinelPassword,
		Password:         cfg.Password,
		DB:               cfg.Db,
	})

	return &LogsRedisClient{
		rdb:                  rdb,
		logLastTickStatusKey: cfg.LogLastTickStatusKey,
	}
}

func (er *LogsRedisClient) GetLogLastIngestedTickStatus(ctx context.Context) (consumedEventLogTick domain.RedisLogsLastIngestedTickStatus, err error) {
	values, err := er.rdb.HGetAll(ctx, er.logLastTickStatusKey).Result()
	if err != nil {
		return domain.RedisLogsLastIngestedTickStatus{}, fmt.Errorf("getting last ingested log tick: %w", err)
	}

	if len(values) == 0 {
		return domain.RedisLogsLastIngestedTickStatus{}, fmt.Errorf("no data found for key [%s]", er.logLastTickStatusKey)
	}

	tickNumber, err := strconv.ParseUint(values["tickNumber"], 10, 32)
	if err != nil {
		return domain.RedisLogsLastIngestedTickStatus{}, fmt.Errorf("parsing tick number: %w", err)
	}

	eventCount, err := strconv.ParseUint(values["count"], 10, 32)
	if err != nil {
		return domain.RedisLogsLastIngestedTickStatus{}, fmt.Errorf("parsing log count: %w", err)
	}

	return domain.RedisLogsLastIngestedTickStatus{
		TickNumber: uint32(tickNumber),
		LogCount:   uint32(eventCount),
	}, nil
}

func (er *LogsRedisClient) Close() error {
	return er.rdb.Close()
}
