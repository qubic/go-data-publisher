package pebbledb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/cockroachdb/pebble"
	"path/filepath"
	"strconv"
)

var ErrNotFound = errors.New("store resource not found")

const maxTickNumber = ^uint32(0)

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

func (ps *Store) SetLastProcessedTick(epoch, tick uint32) error {
	key := []byte{lastProcessedTickPerEpochKey}
	key = binary.BigEndian.AppendUint32(key, epoch)

	var value []byte
	value = binary.BigEndian.AppendUint32(value, tick)

	err := ps.db.Set(key, value, pebble.Sync)
	if err != nil {
		return fmt.Errorf("setting last processed tick: %v", err)
	}

	return nil
}

func (ps *Store) GetLastProcessedTick(epoch uint32) (tick uint32, err error) {
	key := []byte{lastProcessedTickPerEpochKey}
	key = binary.BigEndian.AppendUint32(key, epoch)

	value, closer, err := ps.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, ErrNotFound
	}

	if err != nil {
		return 0, fmt.Errorf("getting last processed tick: %v", err)
	}
	defer closer.Close()

	tick = binary.BigEndian.Uint32(value)

	return tick, nil
}

func (ps *Store) GetLastProcessedTickForAllEpochs() (map[uint32]uint32, error) {
	upperBound := append([]byte{lastProcessedTickPerEpochKey}, []byte(strconv.FormatUint(uint64(maxTickNumber), 10))...)
	iter, err := ps.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{lastProcessedTickPerEpochKey},
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator: %v", err)
	}
	defer iter.Close()

	ticksPerEpoch := make(map[uint32]uint32)
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()

		value, err := iter.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("getting value from iter: %v", err)
		}

		epochNumber := binary.BigEndian.Uint32(key[1:])
		tickNumber := binary.LittleEndian.Uint32(value)
		ticksPerEpoch[epochNumber] = tickNumber
	}

	return ticksPerEpoch, nil
}

func (ps *Store) Close() error {
	return ps.db.Close()
}
