package datatable

import "meteor/internal/common"

type MapDataTable struct {
	table map[common.K]common.V
}

func NewMapDataTable() *MapDataTable {
	return &MapDataTable{
		table: make(map[common.K]common.V),
	}
}

func (m *MapDataTable) Get(key common.K) common.V {
	return m.table[key]
}

func (m *MapDataTable) Put(key common.K, value common.V) error {
	m.table[key] = value
	return nil
}

func (m *MapDataTable) Delete(key common.K) error {
	delete(m.table, key)
	return nil
}

func (m *MapDataTable) Size() (int, error) {
	return len(m.table), nil
}

func (m *MapDataTable) Clear() error {
	m.table = make(map[common.K]common.V)
	return nil
}