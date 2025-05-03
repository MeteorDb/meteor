package storemanager

import (
	"fmt"
	"io"
	"meteor/internal/common"
	"meteor/internal/gsnmanager"
	"meteor/internal/snapshotmanager"
	"meteor/internal/store"
	"meteor/internal/transactionmanager"
	"meteor/internal/walmanager"
)

const (
	MAX_IMMUTABLE_STORES = 2
	USE_WAL = true
)

type StoreManager struct {
	bufferStore        store.Store
	immutableStores    []store.Store
	gsnManager         *gsnmanager.GsnManager
	transactionManager *transactionmanager.TransactionManager
	snapshotManager    *snapshotmanager.SnapshotManager
	walManager         *walmanager.WalManager
}

func NewStoreManager() (*StoreManager, error) {
	walManager, err := walmanager.NewWalManager()
	if err != nil {
		return nil, err
	}

	gsnManager, err := gsnmanager.NewGsnManager(walManager)
	if err != nil {
		return nil, err
	}

	transactionManager, err := transactionmanager.NewTransactionManager(walManager)
	if err != nil {
		return nil, err
	}

	bufferStore := store.NewBufferStore()
	immutableStores := make([]store.Store, 0)

	return &StoreManager{
		bufferStore:        bufferStore,
		immutableStores:    immutableStores,
		gsnManager:         gsnManager,
		transactionManager: transactionManager,
		snapshotManager:    &snapshotmanager.SnapshotManager{},
		walManager:         walManager,
	}, nil
}

func (sm *StoreManager) RecoverStoreFromWal() error {
	for {
		transactionRow, err := sm.walManager.ReadRow()
		if err != nil {
			if err == io.EOF {
				// EOF means we've reached the end of the file, which is expected
				return nil
			}
			return err
		}
		sm.putByTransactionRow(transactionRow, false)
		fmt.Printf("%+v\n", transactionRow)
	}
}

func (sm *StoreManager) Put(key string, value []byte, valueType common.DataType, transactionId uint32) error {
	gsn := sm.gsnManager.GetNewGsn()

	keyObj := common.K{Key: key, Gsn: gsn}
	valueObj := common.V{Type: valueType, Value: value}

	oldValue := sm.bufferStore.Get(key)

	transactionRow := common.NewTransactionRow(transactionId, "PUT", keyObj, oldValue, valueObj)

	return sm.putByTransactionRow(transactionRow, USE_WAL)
}

func (sm *StoreManager) putByTransactionRow(transactionRow *common.TransactionRow, useWal bool) error {
	if useWal {
		err := sm.walManager.AddRow(transactionRow)
		if err != nil {
			return err
		}
	}

	sm.bufferStore.Put(transactionRow.Payload.Key, transactionRow.Payload.NewValue)
	return nil
}

func (sm *StoreManager) Get(key string) (common.V, error) {
	return sm.bufferStore.Get(key), nil
}

func (sm *StoreManager) Delete(key string, transactionId uint32) error {
	gsn := sm.gsnManager.GetNewGsn()

	keyObj := common.K{Key: key, Gsn: gsn}

	oldValue := sm.bufferStore.Get(key)
	tombstone := common.V{Type: common.TypeNull, Value: nil}

	transactionRow := common.NewTransactionRow(transactionId, "DELETE", keyObj, oldValue, tombstone)

	return sm.putByTransactionRow(transactionRow, USE_WAL)
}

func (sm *StoreManager) Size() (int, error) {
	return sm.bufferStore.Size()
}

func (sm *StoreManager) Reset() error {
	return sm.bufferStore.Reset()
}

func (sm *StoreManager) PerformAction(cmd *common.Command) ([]byte, error) {
	var err error
	var val common.V
	switch cmd.Operation {
	case "PUT":
		err = sm.Put(cmd.Args[0], []byte(cmd.Args[1]), common.DataType(cmd.Args[2][0]-'0'), 0)
	case "DELETE":
		err = sm.Delete(cmd.Args[0], 0)
	case "GET":
		val, err = sm.Get(cmd.Args[0])
	}

	if err != nil {
		return nil, err
	}

	res := fmt.Appendf(nil, "value: %s  type: %s\n", val.Value, val.Type)

	return res, nil
}
