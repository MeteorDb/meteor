package store

import "meteor/internal/common"

type Store interface {
	Get(key string) *common.V
	Put(key *common.K, value *common.V) error
	Delete(key string) error
	Size() (int, error)
	Reset() error
	Keys() []string
	// GetLatestGsn returns the latest GSN for a key.
	GetLatestGsn(key string) (uint32, error)
	// GetVersionAtOrBeforeGsn returns the latest version of a key that was created at or before the specified GSN. Required for SNAPSHOT_ISOLATION.
	GetVersionAtOrBeforeGsn(key string, maxGsn uint32) *common.V
	
	// ScanPrefix returns all key-value pairs where key starts with the given prefix
	ScanPrefix(prefix string) map[string]*common.V
	// ScanRange returns all key-value pairs in the lexicographic range [startKey, endKey]
	ScanRange(startKey, endKey string) map[string]*common.V
	// ScanWithFilter returns all key-value pairs that match the filter function
	ScanWithFilter(filterFunc func(string, *common.V) bool) map[string]*common.V
	// CountWithFilter returns the count of keys that match the filter function
	CountWithFilter(filterFunc func(string, *common.V) bool) int
}
