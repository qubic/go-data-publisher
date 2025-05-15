package db

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/pkg/errors"
	"github.com/qubic/status-service/util"
	"log"
	"path/filepath"
	"sort"
	"strconv"
)

var ErrNotFound = errors.New("store resource not found")

const lastProcessedTickKey = "lpt"
const skippedTicksKey = "skipped"

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
	defer closer.Close()

	tick = binary.BigEndian.Uint32(value)
	return tick, nil
}

func (ps *PebbleStore) AddSkippedTick(tick uint32) error {
	skippedTicks, err := ps.loadSkippedTicksSet()
	if err != nil {
		return errors.Wrap(err, "getting skipped ticks")
	}
	util.AddToSet(skippedTicks, fmt.Sprint(tick))
	err = ps.saveSkippedTicksSet(skippedTicks)
	if err != nil {
		return errors.Wrap(err, "saving skipped ticks")
	}
	return nil
}

func (ps *PebbleStore) GetSkippedTicks() ([]uint32, error) {
	skippedTicks, err := ps.loadSkippedTicksSet()
	if err != nil {
		return nil, errors.Wrap(err, "getting skipped ticks")
	}
	tickList := make([]uint32, 0, len(skippedTicks)) // empty array is default return value
	// Iterate over all keys
	for key, val := range skippedTicks {
		if val {
			tickNumber, err := strconv.ParseUint(key, 10, 32)
			if err != nil {
				return nil, errors.Wrapf(err, "error converting [%s] to number", key)
			}
			tickList = append(tickList, uint32(tickNumber))
		}
	}
	// sort tick numbers
	sort.Slice(tickList, func(i, j int) bool { return tickList[i] < tickList[j] })
	return tickList, nil
}

func (ps *PebbleStore) saveSkippedTicksSet(set map[string]bool) error {
	// encode
	buffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(buffer)
	err := encoder.Encode(set)
	if err != nil {
		return errors.Wrap(err, "encoding set")
	}

	// store
	key := []byte(skippedTicksKey)
	err = ps.db.Set(key, buffer.Bytes(), pebble.Sync) // sync to prevent data loss. performance not important.
	if err != nil {
		return errors.Wrap(err, "saving set")
	}
	return nil
}

func (ps *PebbleStore) loadSkippedTicksSet() (map[string]bool, error) {
	// load
	key := []byte(skippedTicksKey)
	value, closer, err := ps.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		log.Printf("[WARN] key [%s] not found.", skippedTicksKey)
		return util.NewSet(), nil
	}
	if err != nil {
		return nil, errors.Wrapf(err, "getting value for key [%s]", lastProcessedTickKey)
	}
	defer closer.Close()

	// decode
	buffer := bytes.NewBuffer(value)
	decoder := gob.NewDecoder(buffer)
	var skippedTicks map[string]bool
	err = decoder.Decode(&skippedTicks)
	if err != nil {
		return nil, errors.Wrap(err, "deserializing skipped ticks")
	}

	return skippedTicks, nil
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
