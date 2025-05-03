package storemanager

import (
	"meteor/internal/common"
	"meteor/internal/snapshotmanager"
	"meteor/internal/store"
	"meteor/internal/transactionmanager"
	"meteor/internal/walmanager"
)

type StoreManager struct {
	bufferStore        store.Store
	immutableStores    []store.Store
	transactionManager *transactionmanager.TransactionManager
	snapshotManager    *snapshotmanager.SnapshotManager
	walManager         *walmanager.WalManager
}

func NewStoreManager() (*StoreManager, error) {
	walManager, err := walmanager.NewWalManager()
	if err != nil {
		return nil, err
	}
	
	err = walManager.RecoverFromWal()
	if err != nil {
		return nil, err
	}
	
	transactionManager, err := transactionmanager.NewTransactionManager()
	if err != nil {
		return nil, err
	}
	return &StoreManager{
		// bufferStore:        &store.BufferStore{},
		// immutableStores:    []store.ImmutableStore{},
		transactionManager: transactionManager,
		snapshotManager:    &snapshotmanager.SnapshotManager{},
		walManager:         walManager,
	}, nil
}


func (sm *StoreManager) Put(key common.K, value common.V, transactionId uint32) error {
	// Get a transaction id from transaction manager
	if transactionId == 0 {
		transactionId = transactionmanager.GetNewTransactionId()
	}
	gsn := common.GetNewGsn()
	// Create a transaction row
	transactionRow := common.NewTransactionRow(gsn, transactionId, "PUT", key, nil, value)

	// Add the row to wal using walManager
	err := sm.walManager.AddRow(transactionRow)
	if err != nil {
		return err
	}
	// Add the row to transaction table using transactionManager
	sm.transactionManager.AddTransaction(transactionRow)

	// Add the key, value to bufferStore using bufferStore.Put(key, value)
	
	// return error or nil to the user
	return nil
}
