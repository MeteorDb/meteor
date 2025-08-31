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
}
