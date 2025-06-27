package db

import (
	"encoding/binary"
	"github.com/cockroachdb/pebble"
	"github.com/pkg/errors"
	"io"
	"log"
	"path/filepath"
)

var ErrNotFound = errors.New("store resource not found")

const epochKey byte = 0x00

type PebbleStore struct {
	db *pebble.DB
}

func NewPebbleStore(storeDir string) (*PebbleStore, error) {
	db, err := pebble.Open(filepath.Join(storeDir, "computors-publisher"), &pebble.Options{})
	if err != nil {
		return nil, errors.Wrap(err, "opening pebble db")
	}

	return &PebbleStore{db: db}, nil
}

func (ps *PebbleStore) SetLastProcessedEpoch(epoch uint32) error {
	key := []byte{epochKey}

	var value []byte
	value = binary.LittleEndian.AppendUint32(value, epoch)

	err := ps.db.Set(key, value, pebble.Sync)
	if err != nil {
		return errors.Wrapf(err, "setting last processed epoch to %d", epoch)
	}
	return nil
}

func (ps *PebbleStore) GetLastProcessedEpoch() (uint32, error) {
	key := []byte{epochKey}

	value, closer, err := ps.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		log.Printf("[WARN]: No last processed epoch has been found.")
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, errors.Wrap(err, "getting last processed epoch")
	}
	defer func(closer io.Closer) {
		err := closer.Close()
		if err != nil {
			log.Printf("[ERROR]: Failed to close database get request.")
		}
	}(closer)

	epoch := binary.LittleEndian.Uint32(value)
	return epoch, nil
}

func (ps *PebbleStore) Close() error {
	return ps.db.Close()
}
