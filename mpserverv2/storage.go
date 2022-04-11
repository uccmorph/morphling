package mpserverv2

import (
	"bytes"

	"github.com/petar/GoLLRB/llrb"
)

const (
	CfDefault string = "default"
)

type Modify struct {
	Data interface{}
}

type Put struct {
	Key   []byte
	Value []byte
}

type Delete struct {
	Key []byte
}

// MemStorage is an in-memory storage engine used for testing. Data is not written to disk, nor sent to other
// nodes. It is intended for testing only.
type MemStorage struct {
	CfDefault *llrb.LLRB
}

func NewMemStorage() *MemStorage {
	return &MemStorage{
		CfDefault: llrb.New(),
	}
}

func (s *MemStorage) Start() error {
	return nil
}

func (s *MemStorage) Stop() error {
	return nil
}

func (s *MemStorage) Write(batch []Modify) error {
	for _, m := range batch {
		switch data := m.Data.(type) {
		case Put:
			item := memItem{data.Key, data.Value, false}
			s.CfDefault.ReplaceOrInsert(item)

		case Delete:
			item := memItem{key: data.Key}
			s.CfDefault.Delete(item)
		}
	}

	return nil
}

func (s *MemStorage) Get(key []byte) []byte {
	item := memItem{key: key}
	result := s.CfDefault.Get(item)

	if result == nil {
		return nil
	}

	return result.(memItem).value
}

func (s *MemStorage) Set(key []byte, value []byte) {
	item := memItem{key, value, true}
	s.CfDefault.ReplaceOrInsert(item)

}

func (s *MemStorage) HasChanged(key []byte) bool {
	item := memItem{key: key}
	result := s.CfDefault.Get(item)
	if result == nil {
		return true
	}

	return !result.(memItem).fresh
}

func (s *MemStorage) Len() int {
	return s.CfDefault.Len()
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
	dst = append(dst[:0], it.value...)
	return dst
}
func (it memItem) Value() ([]byte, error) {
	return it.value, nil
}
func (it memItem) ValueSize() int {
	return len(it.value)
}
func (it memItem) ValueCopy(dst []byte) ([]byte, error) {
	dst = append(dst[:0], it.value...)
	return dst, nil
}

func (it memItem) Less(than llrb.Item) bool {
	other := than.(memItem)
	return bytes.Compare(it.key, other.key) < 0
}
