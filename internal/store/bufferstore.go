package store

import (
	"meteor/internal/common"
	"meteor/internal/datatable"
)

const (
	NUMBER_OF_SHARDS = 4
)

type BufferStore struct {
	tableShards []datatable.DataTable
}

func NewBufferStore() *BufferStore {
	tableShards := make([]datatable.DataTable, NUMBER_OF_SHARDS)

	for i := range NUMBER_OF_SHARDS {
		tableShards[i] = datatable.NewMapDataTable()
	}

	return &BufferStore{
		tableShards: tableShards,
	}
}

func (s *BufferStore) Get(key common.K) common.V {
	shardIndex := key.Hash() % NUMBER_OF_SHARDS
	return s.tableShards[shardIndex].Get(key)
}

func (s *BufferStore) Put(key common.K, value common.V) error {
	shardIndex := key.Hash() % NUMBER_OF_SHARDS
	return s.tableShards[shardIndex].Put(key, value)
}

func (s *BufferStore) Delete(key common.K) error {
	shardIndex := key.Hash() % NUMBER_OF_SHARDS
	return s.tableShards[shardIndex].Delete(key)
}

func (s *BufferStore) Size() (int, error) {
	totalSize := 0
	for _, shard := range s.tableShards {
		size, err := shard.Size()
		if err != nil {
			return 0, err
		}
		totalSize += size
	}
	return totalSize, nil
}

func (s *BufferStore) Reset() error {
	for _, shard := range s.tableShards {
		shard.Clear()
	}
	return nil
}
