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

func (s *BufferStore) Get(key string) *common.V {
	shardIndex := common.HashKey(key) % NUMBER_OF_SHARDS
	return s.tableShards[shardIndex].Get(key)
}

func (s *BufferStore) Put(key *common.K, value *common.V) error {
	shardIndex := common.HashKey(key.Key) % NUMBER_OF_SHARDS
	return s.tableShards[shardIndex].Put(key, value)
}

func (s *BufferStore) Delete(key string) error {
	shardIndex := common.HashKey(key) % NUMBER_OF_SHARDS
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

func (s *BufferStore) Keys() []string {
	keys := make([]string, 0)
	for _, shard := range s.tableShards {
		keys = append(keys, shard.Keys()...)
	}
	return keys
}

func (s *BufferStore) GetLatestGsn(key string) (uint32, error) {
	shardIndex := common.HashKey(key) % NUMBER_OF_SHARDS
	return s.tableShards[shardIndex].GetLatestGsn(key)
}

func (s *BufferStore) GetVersionAtOrBeforeGsn(key string, maxGsn uint32) *common.V {
	shardIndex := common.HashKey(key) % NUMBER_OF_SHARDS
	return s.tableShards[shardIndex].GetVersionAtOrBeforeGsn(key, maxGsn)
}

func (s *BufferStore) ScanPrefix(prefix string) map[string]*common.V {
	result := make(map[string]*common.V)
	for _, shard := range s.tableShards {
		shardResult := shard.ScanPrefix(prefix)
		for k, v := range shardResult {
			result[k] = v
		}
	}
	return result
}

func (s *BufferStore) ScanRange(startKey, endKey string) map[string]*common.V {
	result := make(map[string]*common.V)
	for _, shard := range s.tableShards {
		shardResult := shard.ScanRange(startKey, endKey)
		for k, v := range shardResult {
			result[k] = v
		}
	}
	return result
}

func (s *BufferStore) ScanWithFilter(filterFunc func(string, *common.V) bool) map[string]*common.V {
	result := make(map[string]*common.V)
	for _, shard := range s.tableShards {
		shardResult := shard.ScanWithFilter(filterFunc)
		for k, v := range shardResult {
			result[k] = v
		}
	}
	return result
}

func (s *BufferStore) CountWithFilter(filterFunc func(string, *common.V) bool) int {
	count := 0
	for _, shard := range s.tableShards {
		count += shard.CountWithFilter(filterFunc)
	}
	return count
}
