package storemanager

import (
	"meteor/internal/common"
	"meteor/internal/store"
)

const (
	MAX_IMMUTABLE_STORES = 2
)

// StoreManager manages the LSM tree storage hierarchy
// TODO: Future enhancements for full LSM tree implementation:
// - Buffer store flushing to immutable stores when size threshold reached
// - Immutable store compaction and merging
// - Disk-based storage levels (SSTables)
// - Background compaction processes
// - Bloom filters for efficient key existence checking
// - Block caches for disk read performance
type StoreManager struct {
	BufferStore        store.Store   // In-memory mutable store
	ImmutableStores    []store.Store // In-memory immutable stores (being flushed)
	// TODO: Add disk-based storage levels:
	// DiskStores         []DiskStore   // On-disk immutable stores (SSTables)
	// CompactionManager  *CompactionManager // Manages background compaction
	// BloomFilters       map[string]*BloomFilter // For efficient key lookups
}

func NewStoreManager() (*StoreManager, error) {
	bufferStore := store.NewBufferStore()
	immutableStores := make([]store.Store, 0)

	return &StoreManager{
		BufferStore:        bufferStore,
		ImmutableStores:    immutableStores,
	}, nil
}

// PutTxnRowToBufferStore adds a transaction row to the buffer store
// TODO: Add buffer store size monitoring and automatic flushing:
// - Check buffer store size after each put
// - Trigger flush to immutable store when threshold exceeded
func (sm *StoreManager) PutTxnRowToBufferStore(transactionRow *common.TransactionRow) error {
	sm.BufferStore.Put(transactionRow.Payload.Key, transactionRow.Payload.NewValue)
	
	// TODO: Add size checking and flushing logic:
	// if sm.shouldFlushBufferStore() {
	//     return sm.flushBufferStoreToImmutableStore()
	// }
	
	return nil
}

// Size returns the total size across all storage levels
// TODO: Include sizes from immutable stores and disk stores
func (sm *StoreManager) Size() (int, error) {
	totalSize, err := sm.BufferStore.Size()
	if err != nil {
		return 0, err
	}
	
	// TODO: Add sizes from immutable stores and disk stores:
	// for _, immutableStore := range sm.ImmutableStores {
	//     size, err := immutableStore.Size()
	//     if err != nil {
	//         return 0, err
	//     }
	//     totalSize += size
	// }
	
	return totalSize, nil
}

// Reset resets all storage levels
// TODO: Extend to reset immutable stores and clean up disk files
func (sm *StoreManager) Reset() error {
	err := sm.BufferStore.Reset()
	if err != nil {
		return err
	}
	
	// TODO: Reset immutable stores and clean up disk files:
	// for _, immutableStore := range sm.ImmutableStores {
	//     err := immutableStore.Reset()
	//     if err != nil {
	//         return err
	//     }
	// }
	
	sm.ImmutableStores = sm.ImmutableStores[:0] // Clear slice
	return nil
}

// TODO: Add methods for LSM tree operations:
//
// flushBufferStoreToImmutableStore() error
// - Converts current buffer store to immutable store
// - Creates new empty buffer store
// - Coordinates with snapshot manager
//
// compactImmutableStores() error  
// - Merges multiple immutable stores
// - Removes duplicate keys and tombstones
// - Writes merged data to disk as SSTable
//
// shouldFlushBufferStore() bool
// - Checks if buffer store exceeds size threshold
// - Considers memory pressure and write volume
//
// Get(key string, maxGsn uint32) *common.V
// - Searches across all storage levels for a key at specific GSN
// - Implements proper LSM tree read path
//
// getFromDisk(key string, maxGsn uint32) *common.V
// - Searches disk-based SSTables for key versions
// - Uses bloom filters to avoid unnecessary disk reads
