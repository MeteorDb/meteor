package storemanager

import (
	"meteor/internal/snapshotmanager"
	"meteor/internal/store"
	"meteor/internal/transactionmanager"
	"meteor/internal/walmanager"
)

type StoreManager struct {
	bufferStore        *store.Store
	immutableStores    []*store.Store
	transactionManager *transactionmanager.TransactionManager
	snapshotManager    *snapshotmanager.SnapshotManager
	walManager         *walmanager.WalManager
}

func (sm *StoreManager) Put(key any, value any) error {
	// Get a transaction id from transaction manager
	// Create a WalRow
	// Add the row to wal using walManager
	// Add the row to transaction table using transactionManager
	// Add the key, value to bufferStore using bufferStore.Put(key, value)
	// return error or nil to the user
	return nil
}
