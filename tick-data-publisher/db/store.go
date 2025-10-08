package db

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"path/filepath"

	"github.com/cockroachdb/pebble/v2"
	"github.com/pkg/errors"
)

var ErrNotFound = errors.New("store resource not found")

const lastProcessedTickKey = "lpt"

type PebbleStore struct {
	db *pebble.DB
}

func NewPebbleStore(storeDir string) (*PebbleStore, error) {
	db, err := pebble.Open(filepath.Join(storeDir, "status-service-internal-store"), &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("opening pebble db: %v", err)
	}

	return &PebbleStore{db: db}, nil
}

func (ps *PebbleStore) SetLastProcessedTick(tick uint32) error {
	key := []byte(lastProcessedTickKey)
	var value []byte
	value = binary.BigEndian.AppendUint32(value, tick)

	err := ps.db.Set(key, value, pebble.Sync)
	if err != nil {
		return errors.Wrapf(err, "setting key [%s] to [%d]", lastProcessedTickKey, tick)
	}

	return nil
}

func (ps *PebbleStore) GetLastProcessedTick() (tick uint32, err error) {
	key := []byte(lastProcessedTickKey)

	value, closer, err := ps.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		log.Printf("[WARN] key [%s] not found.", lastProcessedTickKey)
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, errors.Wrapf(err, "getting value for key [%s]", lastProcessedTickKey)
	}
	defer func(closer io.Closer) {
		err := closer.Close()
		if err != nil {
			log.Printf("[ERROR] closing db: %v", err)
		}
	}(closer)

	tick = binary.BigEndian.Uint32(value)
	return tick, nil
}

func (ps *PebbleStore) deleteLastProcessedTick() error {
	key := []byte(lastProcessedTickKey)
	err := ps.db.Delete(key, pebble.Sync)
	if err != nil {
		return errors.Wrapf(err, "deleting key [%s]", lastProcessedTickKey)
	}
	return nil
}

func (ps *PebbleStore) Close() error {
	return ps.db.Close()
}
