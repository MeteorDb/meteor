package datatable

import (
	"errors"
	"meteor/internal/common"
	"sync"
)

type MapDataTable struct {
	m sync.RWMutex
	table map[string]map[uint32]*common.V
}

func NewMapDataTable() *MapDataTable {
	return &MapDataTable{
		m: sync.RWMutex{},
		table: make(map[string]map[uint32]*common.V),
	}
}

func (m *MapDataTable) Get(key string) *common.V {
	m.m.RLock()
	defer m.m.RUnlock()

	gsnMap, ok := m.table[key]
	if !ok {
		return nil
	}

	var maxGsn uint32
	for gsn := range gsnMap {
		if gsn > maxGsn {
			maxGsn = gsn
		}
	}

	return gsnMap[maxGsn]
}

func (m *MapDataTable) Put(key *common.K, value *common.V) error {
	m.m.Lock()
	defer m.m.Unlock()

	gsnMap, ok := m.table[key.Key]
	if !ok {
		gsnMap = make(map[uint32]*common.V)
		m.table[key.Key] = gsnMap
	}
	gsnMap[key.Gsn] = value
	return nil
}

func (m *MapDataTable) Delete(key string) error {
	// We don't need to delete the key from the table, because we put a tombstone in the value in the above layer
	return nil
}

func (m *MapDataTable) Size() (int, error) {
	return len(m.table), nil
}

func (m *MapDataTable) Clear() error {
	m.m.Lock()
	defer m.m.Unlock()

	m.table = make(map[string]map[uint32]*common.V)
	return nil
}

func (m *MapDataTable) Keys() []string {
	m.m.RLock()
	defer m.m.RUnlock()

	keys := make([]string, 0, len(m.table))
	for key := range m.table {
		keys = append(keys, key)
	}
	return keys
}

func (m *MapDataTable) GetLatestGsn(key string) (uint32, error) {
	m.m.RLock()
	defer m.m.RUnlock()

	gsnMap, ok := m.table[key]
	if !ok {
		return 0, errors.New("key not found")
	}

	var maxGsn uint32
	for gsn := range gsnMap {
		if gsn > maxGsn {
			maxGsn = gsn
		}
	}
	return maxGsn, nil
}
