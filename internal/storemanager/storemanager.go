package storemanager

import (
	"errors"
	"fmt"
	"io"
	"meteor/internal/common"
	"meteor/internal/gsnmanager"
	"meteor/internal/snapshotmanager"
	"meteor/internal/store"
	"meteor/internal/transactionmanager"
	"meteor/internal/walmanager"
	"strconv"
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
		fmt.Printf("%v\n", transactionRow)
	}
}

func (sm *StoreManager) Put(cmd *common.Command) ([]byte, error) {

	if len(cmd.Args) < 2 {
		return nil, errors.New("command must have at least 2 arguments")
	}

	key := cmd.Args[0]
	value := []byte(cmd.Args[1])

	var valueType common.DataType
	var transactionId uint32

	if len(cmd.Args) >= 3 {
		valueType = common.DataType(cmd.Args[2][0] - '0')
	} else {
		valueType = common.TypeString
	}

	if len(cmd.Args) == 4 {
		transactionId64Bits, err := strconv.ParseUint(cmd.Args[3], 10, 32)
		if err != nil {
			return nil, errors.New("invalid transaction id")
		}
		transactionId = uint32(transactionId64Bits)
	} else {
		transactionId = sm.transactionManager.GetNewTransactionId()
	}

	gsn := sm.gsnManager.GetNewGsn()

	keyObj := &common.K{Key: key, Gsn: gsn}
	valueObj := &common.V{Type: valueType, Value: value}

	oldValue := sm.bufferStore.Get(key)

	transactionRow := common.NewTransactionRow(transactionId, "PUT", keyObj, oldValue, valueObj)

	err := sm.putByTransactionRow(transactionRow, USE_WAL)
	if err != nil {
		return nil, err
	}

	return []byte("OK"), nil
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

func (sm *StoreManager) Get(cmd *common.Command) ([]byte, error) {
	if len(cmd.Args) != 1 {
		return nil, errors.New("command must have only one argument")
	}

	key := cmd.Args[0]
	
	v := sm.bufferStore.Get(key)

	if v == nil {
		return []byte("-1"), nil
	}

	if v.Type == common.TypeTombstone {
		return []byte("-2"), nil
	}

	return v.Value, nil
}

func (sm *StoreManager) Delete(cmd *common.Command) ([]byte, error) {
	if len(cmd.Args) < 1 {
		return nil, errors.New("command must have at least one argument")
	}

	key := cmd.Args[0]

	var transactionId uint32
	if len(cmd.Args) == 2 {
		transactionId64Bits, err := strconv.ParseUint(cmd.Args[1], 10, 32)
		if err != nil {
			return nil, errors.New("invalid transaction id")
		}
		transactionId = uint32(transactionId64Bits)
	} else {
		transactionId = sm.transactionManager.GetNewTransactionId()
	}

	gsn := sm.gsnManager.GetNewGsn()

	keyObj := &common.K{Key: key, Gsn: gsn}

	oldValue := sm.bufferStore.Get(key)
	tombstone := &common.V{Type: common.TypeTombstone, Value: nil}

	transactionRow := common.NewTransactionRow(transactionId, "DELETE", keyObj, oldValue, tombstone)

	return []byte("OK"), sm.putByTransactionRow(transactionRow, USE_WAL)
}

func (sm *StoreManager) Size() (int, error) {
	return sm.bufferStore.Size()
}

func (sm *StoreManager) Reset() error {
	return sm.bufferStore.Reset()
}

func (sm *StoreManager) PerformAction(cmd *common.Command) ([]byte, error) {
	var res []byte
	var err error
	switch cmd.Operation {
	case "PUT":
		res, err = sm.Put(cmd)
	case "DELETE":
		res, err = sm.Delete(cmd)
	case "GET":
		res, err = sm.Get(cmd)
	}

	if err != nil {
		return nil, err
	}

	res = append(res, '\n')

	return res, nil
}
