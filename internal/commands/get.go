package commands

import (
	"errors"
	"meteor/internal/common"
	"meteor/internal/dbmanager"
	"strconv"
)

func init() {
	Register("GET", []ArgSpec{
		{Name: "key", Type: "string", Required: true, Description: "The key to get"},
		{Name: "transactionId", Type: "uint32", Required: false, Description: "The transaction id to get the key from"},
	}, ensureGet, execGet)
}

type GetArgs struct {
	key                         string
	isPartOfExistingTransaction bool
	transactionId               uint32
}

func ensureGet(dm *dbmanager.DBManager, cmd *common.Command) (*GetArgs, error) {
	if len(cmd.Args) < 1 {
		return nil, errors.New("command must have at least one argument - key")
	}

	getArgs := &GetArgs{
		key:                         cmd.Args[0],
		isPartOfExistingTransaction: false,
		transactionId:               0,
	}

	if len(cmd.Args) == 1 {
		getArgs.transactionId = dm.TransactionManager.GetNewTransactionId()
	}

	if len(cmd.Args) == 2 {
		transactionId64Bits, err := strconv.ParseUint(cmd.Args[1], 10, 32)
		if err != nil {
			return nil, errors.New("invalid transactionId")
		}
		getArgs.transactionId = uint32(transactionId64Bits)

		if dm.TransactionManager.IsNewTransactionId(getArgs.transactionId) {
			return nil, errors.New("transactionId not allowed")
		} else {
			getArgs.isPartOfExistingTransaction = true
		}
	}

	if len(cmd.Args) > 2 {
		return nil, errors.New("command must have at most 2 arguments - key, transactionId")
	}

	return getArgs, nil
}

func execGet(dm *dbmanager.DBManager, getArgs *GetArgs, ctx *CommandContext) ([]byte, error) {
	key := getArgs.key
	transactionId := getArgs.transactionId
	isolationLevel, err := dm.TransactionManager.GetIsolationLevel(transactionId)
	if err != nil {
		return nil, err
	}

	// Acquire read lock based on isolation level
	err = dm.TransactionManager.AcquireReadLock(transactionId, key, isolationLevel)
	if err != nil {
		dm.TransactionManager.ClearTransactionStore(transactionId)
		return nil, err
	}

	// Release read lock immediately for READ_COMMITTED
	defer func() {
		_ = dm.TransactionManager.ReleaseReadLock(transactionId, key, isolationLevel)
	}()

	// Read value using the consolidated ReadValue method that handles the proper read order
	v, err := dm.TransactionManager.ReadValue(transactionId, key, dm.StoreManager.BufferStore, ctx.clientConnection)
	if err != nil {
		dm.TransactionManager.ClearTransactionStore(transactionId)
		return nil, err
	}

	var valueToStore *common.V
	var valueToReturn []byte

	switch {
	case v == nil:
		// Not found
		valueToStore = nil
		valueToReturn = []byte("-1")
	case v.Type == common.TypeTombstone:
		valueToStore = &common.V{Type: common.TypeTombstone, Value: nil}
		valueToReturn = []byte("-2")
	default:
		valueToStore = v
		valueToReturn = v.Value
	}

	// Store read value in transaction store for REPEATABLE_READ and SNAPSHOT_ISOLATION
	// So following GET queries return the same value as the first get query even if the value is changed by other transactions
	if isolationLevel == common.TXN_ISOLATION_REPEATABLE_READ || 
	   isolationLevel == common.TXN_ISOLATION_SNAPSHOT_ISOLATION {
		gsn, _ := dm.StoreManager.BufferStore.GetLatestGsn(key)
		keyObj := &common.K{Key: key, Gsn: gsn}
		transactionRow := common.NewTransactionRow(transactionId, common.DB_OP_GET, common.TRANSACTION_STATE_QUEUED, keyObj, nil, valueToStore)
		err := dm.TransactionManager.AddTransaction(transactionRow, ctx.clientConnection)
		if err != nil {
			dm.TransactionManager.ClearTransactionStore(transactionId)
			return nil, err
		}
		_ = dm.AddTransactionToWal(transactionRow) // Optional
	}

	return valueToReturn, nil
}
