package db

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/pkg/errors"
	"github.com/qubic/go-data-publisher/status-service/domain"
	"github.com/qubic/go-data-publisher/status-service/util"
	"google.golang.org/protobuf/proto"
	"log"
	"path/filepath"
	"sort"
	"strconv"
)

var ErrNotFound = errors.New("store resource not found")

const lastProcessedTickKey = "lpt"
const skippedTicksKey = "skipped"
const processingStatusKey = "status"

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

func (ps *PebbleStore) GetLastProcessedTick() (uint32, error) {
	return ps.getUint32(lastProcessedTickKey)
}

func (ps *PebbleStore) SetLastProcessedTick(tick uint32) error {
	return ps.setUint32(lastProcessedTickKey, tick)
}

func (ps *PebbleStore) SetSourceStatus(status *domain.Status) error {
	return ps.save(processingStatusKey, status)
}

func (ps *PebbleStore) GetSourceStatus() (*domain.Status, error) {
	var target *domain.Status
	err := ps.load(processingStatusKey, &target)
	if err != nil {
		return nil, errors.Wrap(err, "loading processing status")
	}
	return target, nil
}

func (ps *PebbleStore) AddSkippedTick(tick uint32) error {
	skippedTicks, err := ps.loadSkippedTicksSet()
	if err != nil {
		return errors.Wrap(err, "getting skipped ticks")
	}
	util.AddToSet(skippedTicks, fmt.Sprint(tick))
	err = ps.save(skippedTicksKey, skippedTicks)
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

func (ps *PebbleStore) loadSkippedTicksSet() (map[string]bool, error) {
	var result map[string]bool
	err := ps.load(skippedTicksKey, &result)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			log.Println("[WARN] Skipped ticks not found.")
			return util.NewSet(), nil
		} else {
			return nil, err
		}
	}
	return result, nil
}

func (ps *PebbleStore) save(keyStr string, source any) error {
	buffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(buffer)
	err := encoder.Encode(source)
	if err != nil {
		return errors.Wrap(err, "encoding object")
	}

	// store
	key := []byte(keyStr)
	err = ps.db.Set(key, buffer.Bytes(), pebble.Sync) // sync to prevent data loss. lower performance.
	if err != nil {
		return errors.Wrap(err, "saving object")
	}
	return nil
}

func (ps *PebbleStore) load(keyStr string, target any) error {
	key := []byte(keyStr)
	value, closer, err := ps.db.Get(key)
	if err != nil {
		return errors.Wrapf(err, "getting value for key [%s]", keyStr)
	}
	defer closer.Close()

	// decode
	buffer := bytes.NewBuffer(value)
	decoder := gob.NewDecoder(buffer)
	err = decoder.Decode(target)
	if err != nil {
		return errors.Wrapf(err, "deserializing value for key [%s]", keyStr)
	}
	return nil
}

func (ps *PebbleStore) saveProto(keyStr string, source proto.Message) error {
	marshalled, err := proto.Marshal(source)
	if err != nil {
		return errors.Wrap(err, "marshalling object")
	}

	// store
	key := []byte(keyStr)
	err = ps.db.Set(key, marshalled, pebble.Sync) // sync to prevent data loss. lower performance.
	if err != nil {
		return errors.Wrap(err, "saving object")
	}
	return nil
}

func (ps *PebbleStore) loadProto(keyStr string, target proto.Message) error {
	key := []byte(keyStr)
	value, closer, err := ps.db.Get(key)
	if err != nil {
		return errors.Wrapf(err, "getting value for key [%s]", keyStr)
	}
	defer closer.Close()

	// decode
	err = proto.Unmarshal(value, target)
	if err != nil {
		return errors.Wrapf(err, "unmarshalling value for key [%s]", keyStr)
	}
	return nil
}

func (ps *PebbleStore) setUint32(keyStr string, value uint32) error {
	key := []byte(keyStr)
	var val []byte
	val = binary.BigEndian.AppendUint32(val, value)

	err := ps.db.Set(key, val, pebble.Sync)
	if err != nil {
		return errors.Wrapf(err, "setting key [%s] to [%d]", keyStr, value)
	}

	return nil
}

func (ps *PebbleStore) getUint32(keyStr string) (value uint32, err error) {
	key := []byte(keyStr)

	binVal, closer, err := ps.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		log.Printf("[WARN] key [%s] not found.", keyStr)
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, errors.Wrapf(err, "getting value for key [%s]", keyStr)
	}
	defer closer.Close()

	value = binary.BigEndian.Uint32(binVal)
	return value, nil
}

func (ps *PebbleStore) Close() error {
	return ps.db.Close()
}
