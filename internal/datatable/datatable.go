package datatable

import "meteor/internal/common"

type DataTable interface {
	Get(key common.K) common.V
	Put(key common.K, value common.V) error
	Delete(key common.K) error
	Size() (int, error)
	Clear() error
}
