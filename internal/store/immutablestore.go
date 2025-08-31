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

func (s *ImmutableStore) Get(key string) *common.V {
	return s.table.Get(key)
}

func (s *ImmutableStore) Put(key *common.K, value *common.V) error {
	return s.table.Put(key, value)
}

func (s *ImmutableStore) Delete(key string) error {
	return s.table.Delete(key)
}

func (s *ImmutableStore) Size() (int, error) {
	return s.table.Size()
}

func (s *ImmutableStore) Reset() error {
	return s.table.Clear()
}

// Keys returns all keys in the immutable store
func (s *ImmutableStore) Keys() []string {
	return s.table.Keys()
}

// GetLatestGsn returns the latest GSN for a key in the immutable store
func (s *ImmutableStore) GetLatestGsn(key string) (uint32, error) {
	return s.table.GetLatestGsn(key)
}

// GetVersionAtOrBeforeGsn returns the latest version of a key that was created at or before the specified GSN
func (s *ImmutableStore) GetVersionAtOrBeforeGsn(key string, maxGsn uint32) *common.V {
	return s.table.GetVersionAtOrBeforeGsn(key, maxGsn)
}
