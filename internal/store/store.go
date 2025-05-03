package store

import "meteor/internal/common"

type Store interface {
	Get(key string) common.V
	Put(key common.K, value common.V) error
	Delete(key string) error
	Size() (int, error)
	Reset() error
}
