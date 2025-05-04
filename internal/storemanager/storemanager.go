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
	"slices"
	"strconv"
)

const (
	MAX_IMMUTABLE_STORES = 2
	USE_WAL              = true
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
	activeTransactionIds := make([]uint32, 0)

	err := readWalRows(sm.walManager, func(transactionRow *common.TransactionRow) {
		fmt.Printf("transactionRow: %v\n", transactionRow)
		if transactionRow.State == common.TRANSACTION_STATE_COMMIT {
			activeTransactionIds = append(activeTransactionIds, transactionRow.TransactionId)
		}
	})

	if err != nil {
		return err
	}

	sm.walManager.ResetOffsetToFirstRow()

	err = readWalRows(sm.walManager, func(transactionRow *common.TransactionRow) {
		transactionIdx := slices.Index(activeTransactionIds, transactionRow.TransactionId)
		if transactionIdx == -1 {
			return
		}

		if transactionRow.State == common.TRANSACTION_STATE_ROLLBACK {
			activeTransactionIds = slices.Delete(activeTransactionIds, transactionIdx, transactionIdx + 1)
		}

		if !slices.Contains([]string{common.DB_OP_PUT, common.DB_OP_DELETE}, transactionRow.Operation) {
			return
		}

		sm.putByTransactionRow(transactionRow)
	})

	if err != nil {
		return err
	}

	return nil
}

func readWalRows(walManager *walmanager.WalManager, callback func(transactionRow *common.TransactionRow)) error {
	for {
		transactionRow, err := walManager.ReadRow()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		callback(transactionRow)
	}
}

// TODO: While in transaction, mutations are queued while GET fetches from buffer store. GET with transaction id should also check its own transaction queue to fetch the latest value.

func (sm *StoreManager) Begin(cmd *common.Command) ([]byte, error) {
	if len(cmd.Args) != 0 {
		return nil, errors.New("command must have no arguments")
	}

	transactionId := sm.transactionManager.GetNewTransactionId()

	gsn := sm.gsnManager.GetNewGsn()
	key := &common.K{Key: common.TypeKeyNull, Gsn: gsn}

	transactionRow := common.NewTransactionRow(transactionId, common.DB_OP_BEGIN, common.TRANSACTION_STATE_QUEUED, key, nil, nil)

	err := sm.transactionManager.AddTransaction(transactionRow, cmd.Connection)
	if err != nil {
		return nil, err
	}

	err = sm.addTransactionToWal(transactionRow)
	if err != nil {
		return nil, err
	}

	return []byte(strconv.FormatUint(uint64(transactionId), 10)), nil
}

func (sm *StoreManager) Commit(cmd *common.Command) ([]byte, error) {
	if len(cmd.Args) != 1 {
		return nil, errors.New("command must have one argument")
	}

	transactionId64Bits, err := strconv.ParseUint(cmd.Args[0], 10, 32)
	if err != nil {
		return nil, errors.New("invalid transaction id")
	}
	transactionId := uint32(transactionId64Bits)

	gsn := sm.gsnManager.GetNewGsn()
	key := &common.K{Key: common.TypeKeyNull, Gsn: gsn}

	transactionRow := common.NewTransactionRow(transactionId, common.DB_OP_COMMIT, common.TRANSACTION_STATE_COMMIT, key, nil, nil)
	
	err = sm.transactionManager.AddTransaction(transactionRow, cmd.Connection)
	if err != nil {
		return nil, err
	}

	err = sm.addTransactionToWal(transactionRow)
	if err != nil {
		return nil, err
	}

	transactionRows := sm.transactionManager.GetTransactionRows(transactionId)

	for _, transactionRow := range transactionRows {
		sm.putByTransactionRow(transactionRow)
	}

	sm.transactionManager.ClearTransactionRows(transactionId)

	return []byte("OK"), nil
}

func (sm *StoreManager) Rollback(cmd *common.Command) ([]byte, error) {
	if len(cmd.Args) != 1 {
		return nil, errors.New("command must have one argument")
	}
	
	transactionId64Bits, err := strconv.ParseUint(cmd.Args[0], 10, 32)
	if err != nil {
		return nil, errors.New("invalid transaction id")
	}
	transactionId := uint32(transactionId64Bits)

	gsn := sm.gsnManager.GetNewGsn()
	key := &common.K{Key: common.TypeKeyNull, Gsn: gsn}

	transactionRow := common.NewTransactionRow(transactionId, common.DB_OP_ROLLBACK, common.TRANSACTION_STATE_ROLLBACK, key, nil, nil)

	err = sm.transactionManager.AddTransaction(transactionRow, cmd.Connection)
	if err != nil {
		return nil, err
	}
	
	err = sm.addTransactionToWal(transactionRow)
	if err != nil {
		return nil, err
	}

	sm.transactionManager.ClearTransactionRows(transactionId)

	return []byte("OK"), nil
}

func (sm *StoreManager) Put(cmd *common.Command) ([]byte, error) {

	if len(cmd.Args) < 3 {
		return nil, errors.New("command must have at least 3 arguments - key, value, isPartOfExistingTransaction")
	}

	key := cmd.Args[0]
	value := []byte(cmd.Args[1])

	var valueType common.DataType = common.TypeString
	var transactionId uint32

	// if len(cmd.Args) >= 3 {
	// 	valueType = common.DataType(cmd.Args[2][0] - '0')
	// } else {
	// 	valueType = common.TypeString
	// }

	isPartOfExistingTransaction, err := strconv.ParseBool(cmd.Args[2])
	if err != nil {
		return nil, errors.New("cannot parse isPartOfExistingTransaction")
	}

	if isPartOfExistingTransaction {
		if len(cmd.Args) < 4 {
			return nil, errors.New("command must contain transaction id if already part of a transaction")
		}
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

	transactionState := common.TRANSACTION_STATE_COMMIT
	if isPartOfExistingTransaction {
		transactionState = common.TRANSACTION_STATE_QUEUED
	}

	transactionRow := common.NewTransactionRow(transactionId, common.DB_OP_PUT, transactionState, keyObj, oldValue, valueObj)

	err = sm.transactionManager.AddTransaction(transactionRow, cmd.Connection)
	if err != nil {
		return nil, err
	}

	err = sm.addTransactionToWal(transactionRow)
	if err != nil {
		return nil, err
	}

	if !isPartOfExistingTransaction {
		err = sm.putByTransactionRow(transactionRow)
		if err != nil {
			return nil, err
		}
		return []byte("OK"), nil
	}

	return []byte("QUEUED"), nil
}

func (sm *StoreManager) addTransactionToWal(transactionRow *common.TransactionRow) error {
	if !USE_WAL {
		return nil
	}
	fmt.Printf("%v\n", transactionRow)
	return sm.walManager.AddRow(transactionRow)
}

func (sm *StoreManager) putByTransactionRow(transactionRow *common.TransactionRow) error {
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
	if len(cmd.Args) < 2 {
		return nil, errors.New("command must have at least two arguments")
	}

	key := cmd.Args[0]

	isPartOfExistingTransaction, err := strconv.ParseBool(cmd.Args[1])
	if err != nil {
		return nil, errors.New("cannot parse isPartOfExistingTransaction")
	}

	var transactionId uint32

	if isPartOfExistingTransaction {
		if len(cmd.Args) < 3 {
			return nil, errors.New("command must contain transaction id if already part of a transaction")
		}
		transactionId64Bits, err := strconv.ParseUint(cmd.Args[2], 10, 32)
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

	transactionState := common.TRANSACTION_STATE_COMMIT
	if isPartOfExistingTransaction {
		transactionState = common.TRANSACTION_STATE_QUEUED
	}

	transactionRow := common.NewTransactionRow(transactionId, common.DB_OP_DELETE, transactionState, keyObj, oldValue, tombstone)

	err = sm.transactionManager.AddTransaction(transactionRow, cmd.Connection)
	if err != nil {
		return nil, err
	}

	err = sm.addTransactionToWal(transactionRow)
	if err != nil {
		return nil, err
	}

	if !isPartOfExistingTransaction {
		err = sm.putByTransactionRow(transactionRow)
		if err != nil {
			return nil, err
		}
		return []byte("OK"), nil
	}

	return []byte("QUEUED"), nil
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
	case "BEGIN":
		res, err = sm.Begin(cmd)
	case "COMMIT":
		res, err = sm.Commit(cmd)
	case "ROLLBACK":
		res, err = sm.Rollback(cmd)
	}

	if err != nil {
		return nil, err
	}

	res = append(res, '\n')

	return res, nil
}
