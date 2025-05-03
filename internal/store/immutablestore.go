package store

import (
	"meteor/internal/common"
	"meteor/internal/datatable"
)

type ImmutableStore struct {
	table datatable.DataTable
}

func NewImmutableStore() *ImmutableStore {
	return &ImmutableStore{
		table: datatable.NewMapDataTable(),
	}
}

func (s *ImmutableStore) Get(key common.K) common.V {
	return s.table.Get(key)
}

func (s *ImmutableStore) Put(key common.K, value common.V) error {
	return s.table.Put(key, value)
}

func (s *ImmutableStore) Delete(key common.K) error {
	return s.table.Delete(key)
}

func (s *ImmutableStore) Size() (int, error) {
	return s.table.Size()
}

func (s *ImmutableStore) Reset() error {
	return s.table.Clear()
}
