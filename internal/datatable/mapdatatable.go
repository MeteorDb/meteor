package datatable

import "meteor/internal/common"

type MapDataTable struct {
	table map[string]map[uint32]common.V
}

func NewMapDataTable() *MapDataTable {
	return &MapDataTable{
		table: make(map[string]map[uint32]common.V),
	}
}

func (m *MapDataTable) Get(key string) common.V {
	gsnMap, ok := m.table[key]
	if !ok {
		return common.V{}
	}

	var maxGsn uint32
	for gsn := range gsnMap {
		if gsn > maxGsn {
			maxGsn = gsn
		}
	}

	return gsnMap[maxGsn]
}

func (m *MapDataTable) Put(key common.K, value common.V) error {
	gsnMap, ok := m.table[key.Key]
	if !ok {
		gsnMap = make(map[uint32]common.V)
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
	m.table = make(map[string]map[uint32]common.V)
	return nil
}
