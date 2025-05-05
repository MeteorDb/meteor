package datatable

import "meteor/internal/common"

type DataTable interface {
	Get(key string) *common.V
	Put(key *common.K, value *common.V) error
	Delete(key string) error
	Size() (int, error)
	Clear() error
	Keys() []string
	GetLatestGsn(key string) (uint32, error)
}
