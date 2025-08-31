package commands

import (
	"errors"
	"meteor/internal/common"
	"meteor/internal/dbmanager"
	"strconv"
)

func init() {
	Register("PUT", []ArgSpec{
			{Name: "key", Type: "string", Required: true, Description: "The key to set"},
			{Name: "value", Type: "string", Required: true, Description: "The value to set"},
			{Name: "transactionId", Type: "uint32", Required: false, Description: "The transaction id to put the key and value to"},
		}, ensurePut, execPut)
}

type PutArgs struct {
	key string
	value string
	isPartOfExistingTransaction bool
	transactionId uint32
}

func ensurePut(dm *dbmanager.DBManager, cmd *common.Command) (*PutArgs, error) {
	argLen := len(cmd.Args)
	
	if argLen < 2 {
		return nil, errors.New("command must have at least 2 arguments - key, value")
	}

	putArgs := &PutArgs{
		key: cmd.Args[0],
		value: cmd.Args[1],
		isPartOfExistingTransaction: false,
		transactionId: 0,
	}

	// if no transactionId, get a new one
	if argLen == 2 {
		putArgs.transactionId = dm.TransactionManager.GetNewTransactionId()
	}

	// if transactionId, check if it is part of an existing transaction
	if argLen == 3 {
		transactionId64Bits, err := strconv.ParseUint(cmd.Args[2], 10, 32)
		if err != nil {
			return nil, errors.New("invalid transactionId")
		}
		putArgs.transactionId = uint32(transactionId64Bits)

		if dm.TransactionManager.IsNewTransactionId(putArgs.transactionId) {
			return nil, errors.New("transactionId not allowed")
		} else {
			putArgs.isPartOfExistingTransaction = true
		}
	}

	if argLen > 3 {
		return nil, errors.New("command must have at most 3 arguments - key, value, transactionId")
	}

	return putArgs, nil
}

func execPut(dm *dbmanager.DBManager, putArgs *PutArgs, ctx *CommandContext) ([]byte, error) {
	key := putArgs.key
	value := []byte(putArgs.value)
	isPartOfExistingTransaction := putArgs.isPartOfExistingTransaction
	transactionId := putArgs.transactionId

	isolationLevel, err := dm.TransactionManager.GetIsolationLevel(transactionId)
	if err != nil {
		return nil, err
	}

	// Acquire write lock. But this lock is not released immediately for non-transactional operations since lock should be released only after commit or rollback.
	err = dm.TransactionManager.AcquireWriteLock(transactionId, key, isolationLevel)
	if err != nil {
		dm.TransactionManager.ClearTransactionStore(transactionId)
		return nil, err
	}

	var valueType common.DataType = common.TypeString

	gsn := dm.GsnManager.GetNewGsn()

	keyObj := &common.K{Key: key, Gsn: gsn}
	valueObj := &common.V{Type: valueType, Value: value}

	// Get old value using the correct read order (transaction store first, then buffer store)
	var oldValue *common.V
	// Use ReadValue to get the proper old value considering isolation level
	oldValue, err = dm.TransactionManager.ReadValue(transactionId, key, dm.StoreManager.BufferStore, ctx.clientConnection)
	if err != nil {
		dm.TransactionManager.ClearTransactionStore(transactionId)
		return nil, err
	}

	// Validate write based on isolation level
	err = dm.TransactionManager.ValidateWrite(transactionId, key, dm.StoreManager.BufferStore, ctx.clientConnection)
	if err != nil {
		dm.TransactionManager.ClearTransactionStore(transactionId)
		return nil, err
	}

	// if not part of existing transaction, commit immediately as single operation
	transactionState := common.TRANSACTION_STATE_COMMIT

	// if part of existing transaction, queue the operation
	if isPartOfExistingTransaction {
		transactionState = common.TRANSACTION_STATE_QUEUED
	}

	transactionRow := common.NewTransactionRow(transactionId, common.DB_OP_PUT, transactionState, keyObj, oldValue, valueObj)

	// the operation is stored in transaction store (queued)
	err = dm.TransactionManager.AddTransaction(transactionRow, ctx.clientConnection)
	if err != nil {
		dm.TransactionManager.ClearTransactionStore(transactionId)
		return nil, err
	}

	err = dm.AddTransactionToWal(transactionRow)
	if err != nil {
		dm.TransactionManager.ClearTransactionStore(transactionId)
		return nil, err
	}

	// if not part of existing transaction, put the transaction row to the buffer store as single operation
	if !isPartOfExistingTransaction {
		err = dm.StoreManager.PutTxnRowToBufferStore(transactionRow)
		if err != nil {
			dm.TransactionManager.ClearTransactionStore(transactionId)
			return nil, err
		}
		
		// Release locks immediately for non-transactional operations
		dm.TransactionManager.ClearTransactionStore(transactionId)
		
		return []byte("OK"), nil
	}

	return []byte("QUEUED"), nil
}