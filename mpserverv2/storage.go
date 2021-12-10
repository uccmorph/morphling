package mpserverv2

import (
	"bytes"
	"fmt"

	"github.com/dgraph-io/badger/y"
	"github.com/petar/GoLLRB/llrb"
)

type Storage interface {
	Start() error
	Stop() error
	Write(batch []Modify) error
	Reader() (StorageReader, error)
}

type StorageReader interface {
	// When the key doesn't exist, return nil for the value
	GetCF(cf string, key []byte) ([]byte, error)
	IterCF(cf string) DBIterator
	Close()
}

type DBIterator interface {
	// Item returns pointer to the current key-value pair.
	Item() DBItem
	// Valid returns false when iteration is done.
	Valid() bool
	// Next would advance the iterator by one. Always check it.Valid() after a Next()
	// to ensure you have access to a valid it.Item().
	Next()
	// Seek would seek to the provided key if present. If absent, it would seek to the next smallest key
	// greater than provided.
	Seek([]byte)

	// Close the iterator
	Close()
}

type DBItem interface {
	// Key returns the key.
	Key() []byte
	// KeyCopy returns a copy of the key of the item, writing it to dst slice.
	// If nil is passed, or capacity of dst isn't sufficient, a new slice would be allocated and
	// returned.
	KeyCopy(dst []byte) []byte
	// Value retrieves the value of the item.
	Value() ([]byte, error)
	// ValueSize returns the size of the value.
	ValueSize() int
	// ValueCopy returns a copy of the value of the item from the value log, writing it to dst slice.
	// If nil is passed, or capacity of dst isn't sufficient, a new slice would be allocated and
	// returned.
	ValueCopy(dst []byte) ([]byte, error)
}

const (
	CfDefault string = "default"
	CfWrite   string = "write"
	CfLock    string = "lock"
)

type Modify struct {
	Data interface{}
}

type Put struct {
	Key   []byte
	Value []byte
	Cf    string
}

type Delete struct {
	Key []byte
	Cf  string
}

// MemStorage is an in-memory storage engine used for testing. Data is not written to disk, nor sent to other
// nodes. It is intended for testing only.
type MemStorage struct {
	CfDefault *llrb.LLRB
	CfLock    *llrb.LLRB
	CfWrite   *llrb.LLRB
}

func NewMemStorage() *MemStorage {
	return &MemStorage{
		CfDefault: llrb.New(),
		CfLock:    llrb.New(),
		CfWrite:   llrb.New(),
	}
}

func (s *MemStorage) Start() error {
	return nil
}

func (s *MemStorage) Stop() error {
	return nil
}

func (s *MemStorage) Reader() (StorageReader, error) {
	return &memReader{s, 0}, nil
}

func (s *MemStorage) Write(batch []Modify) error {
	for _, m := range batch {
		switch data := m.Data.(type) {
		case Put:
			item := memItem{data.Key, data.Value, false}
			switch data.Cf {
			case CfDefault:
				s.CfDefault.ReplaceOrInsert(item)
			case CfLock:
				s.CfLock.ReplaceOrInsert(item)
			case CfWrite:
				s.CfWrite.ReplaceOrInsert(item)
			}
		case Delete:
			item := memItem{key: data.Key}
			switch data.Cf {
			case CfDefault:
				s.CfDefault.Delete(item)
			case CfLock:
				s.CfLock.Delete(item)
			case CfWrite:
				s.CfWrite.Delete(item)
			}
		}
	}

	return nil
}

func (s *MemStorage) Get(cf string, key []byte) []byte {
	item := memItem{key: key}
	var result llrb.Item
	switch cf {
	case CfDefault:
		result = s.CfDefault.Get(item)
	case CfLock:
		result = s.CfLock.Get(item)
	case CfWrite:
		result = s.CfWrite.Get(item)
	}

	if result == nil {
		return nil
	}

	return result.(memItem).value
}

func (s *MemStorage) Set(cf string, key []byte, value []byte) {
	item := memItem{key, value, true}
	switch cf {
	case CfDefault:
		s.CfDefault.ReplaceOrInsert(item)
	case CfLock:
		s.CfLock.ReplaceOrInsert(item)
	case CfWrite:
		s.CfWrite.ReplaceOrInsert(item)
	}
}

func (s *MemStorage) HasChanged(cf string, key []byte) bool {
	item := memItem{key: key}
	var result llrb.Item
	switch cf {
	case CfDefault:
		result = s.CfDefault.Get(item)
	case CfLock:
		result = s.CfLock.Get(item)
	case CfWrite:
		result = s.CfWrite.Get(item)
	}
	if result == nil {
		return true
	}

	return !result.(memItem).fresh
}

func (s *MemStorage) Len(cf string) int {
	switch cf {
	case CfDefault:
		return s.CfDefault.Len()
	case CfLock:
		return s.CfLock.Len()
	case CfWrite:
		return s.CfWrite.Len()
	}

	return -1
}

// memReader is a StorageReader which reads from a MemStorage.
type memReader struct {
	inner     *MemStorage
	iterCount int
}

func (mr *memReader) GetCF(cf string, key []byte) ([]byte, error) {
	item := memItem{key: key}
	var result llrb.Item
	switch cf {
	case CfDefault:
		result = mr.inner.CfDefault.Get(item)
	case CfLock:
		result = mr.inner.CfLock.Get(item)
	case CfWrite:
		result = mr.inner.CfWrite.Get(item)
	default:
		return nil, fmt.Errorf("mem-server: bad CF %s", cf)
	}

	if result == nil {
		return nil, nil
	}

	return result.(memItem).value, nil
}

func (mr *memReader) IterCF(cf string) DBIterator {
	var data *llrb.LLRB
	switch cf {
	case CfDefault:
		data = mr.inner.CfDefault
	case CfLock:
		data = mr.inner.CfLock
	case CfWrite:
		data = mr.inner.CfWrite
	default:
		return nil
	}

	mr.iterCount += 1
	min := data.Min()
	if min == nil {
		return &memIter{data, memItem{}, mr}
	}
	return &memIter{data, min.(memItem), mr}
}

func (r *memReader) Close() {
	if r.iterCount > 0 {
		panic("Unclosed iterator")
	}
}

type memIter struct {
	data   *llrb.LLRB
	item   memItem
	reader *memReader
}

func (it *memIter) Item() DBItem {
	return it.item
}
func (it *memIter) Valid() bool {
	return it.item.key != nil
}
func (it *memIter) Next() {
	first := true
	oldItem := it.item
	it.item = memItem{}
	it.data.AscendGreaterOrEqual(oldItem, func(item llrb.Item) bool {
		// Skip the first item, which will be it.item
		if first {
			first = false
			return true
		}

		it.item = item.(memItem)
		return false
	})
}
func (it *memIter) Seek(key []byte) {
	it.item = memItem{}
	it.data.AscendGreaterOrEqual(memItem{key: key}, func(item llrb.Item) bool {
		it.item = item.(memItem)

		return false
	})
}

func (it *memIter) Close() {
	it.reader.iterCount -= 1
}

type memItem struct {
	key   []byte
	value []byte
	fresh bool
}

func (it memItem) Key() []byte {
	return it.key
}
func (it memItem) KeyCopy(dst []byte) []byte {
	return y.SafeCopy(dst, it.key)
}
func (it memItem) Value() ([]byte, error) {
	return it.value, nil
}
func (it memItem) ValueSize() int {
	return len(it.value)
}
func (it memItem) ValueCopy(dst []byte) ([]byte, error) {
	return y.SafeCopy(dst, it.value), nil
}

func (it memItem) Less(than llrb.Item) bool {
	other := than.(memItem)
	return bytes.Compare(it.key, other.key) < 0
}
