package domain

import "time"

type RedisConsumedEventLogTick struct {
	Total     int
	Stored    int
	Dropped   int
	Timestamp time.Time
}
