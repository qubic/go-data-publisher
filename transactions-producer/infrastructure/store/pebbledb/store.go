package pebbledb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/qubic/transactions-producer/entities"
	"path/filepath"
)

const lastProcessedTickPerEpochKey = 0x00

type Store struct {
	db *pebble.DB
}

func NewProcessorStore(storeDir string) (*Store, error) {
	db, err := pebble.Open(filepath.Join(storeDir, "tx-processor-store"), &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("opening pebble db: %v", err)
	}

	return &Store{db: db}, nil
}

func (ps *Store) SetLastProcessedTick(tick uint32) error {
	key := []byte{lastProcessedTickPerEpochKey}
	key = binary.BigEndian.AppendUint32(key, 0)

	var value []byte
	value = binary.BigEndian.AppendUint32(value, tick)

	err := ps.db.Set(key, value, pebble.Sync)
	if err != nil {
		return fmt.Errorf("setting last processed tick: %v", err)
	}

	return nil
}

func (ps *Store) GetLastProcessedTick() (tick uint32, err error) {
	key := []byte{lastProcessedTickPerEpochKey}
	key = binary.BigEndian.AppendUint32(key, 0)

	value, closer, err := ps.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, entities.ErrStoreEntityNotFound
	}

	if err != nil {
		return 0, fmt.Errorf("getting last processed tick: %v", err)
	}
	defer closer.Close()

	tick = binary.BigEndian.Uint32(value)

	return tick, nil
}

func (ps *Store) Close() error {
	return ps.db.Close()
}
