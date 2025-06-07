package storemanager

import (
	"meteor/internal/common"
	"meteor/internal/store"
)

const (
	MAX_IMMUTABLE_STORES = 2
)

type StoreManager struct {
	BufferStore        store.Store
	ImmutableStores    []store.Store	
}

func NewStoreManager() (*StoreManager, error) {
	bufferStore := store.NewBufferStore()
	immutableStores := make([]store.Store, 0)

	return &StoreManager{
		BufferStore:        bufferStore,
		ImmutableStores:    immutableStores,
	}, nil
}

func (sm *StoreManager) PutTxnRowToBufferStore(transactionRow *common.TransactionRow) error {
	sm.BufferStore.Put(transactionRow.Payload.Key, transactionRow.Payload.NewValue)
	return nil
}

func (sm *StoreManager) Size() (int, error) {
	return sm.BufferStore.Size()
}

func (sm *StoreManager) Reset() error {
	return sm.BufferStore.Reset()
}
