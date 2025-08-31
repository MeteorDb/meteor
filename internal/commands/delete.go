package commands

import (
	"errors"
	"meteor/internal/common"
	"meteor/internal/dbmanager"
	"strconv"
)

func init() {
	Register("DELETE", []ArgSpec{
		{ Name: "key", Type: "string", Required: true, Description: "The key to delete" },
		{ Name: "transactionId", Type: "uint32", Required: false, Description: "The transaction id to delete" },
	}, ensureDelete, execDelete)
}

type DeleteArgs struct {
	key string
	transactionId uint32
	isPartOfExistingTransaction bool
}

func ensureDelete(dm *dbmanager.DBManager, cmd *common.Command) (*DeleteArgs, error) {
	argLen := len(cmd.Args)
	
	if argLen < 1 {
		return nil, errors.New("command must have at least one argument - key")
	}

	deleteArgs := &DeleteArgs{
		key: cmd.Args[0],
		transactionId: 0,
		isPartOfExistingTransaction: false,
	}

	if argLen == 1 {
		deleteArgs.transactionId = dm.TransactionManager.GetNewTransactionId()
	}

	if argLen == 2 {
		transactionId64Bits, err := strconv.ParseUint(cmd.Args[1], 10, 32)
		if err != nil {
			return nil, errors.New("invalid transactionId")
		}
		deleteArgs.transactionId = uint32(transactionId64Bits)
		
		// if new transaction, client not allowed to specify transactionId
		// it will be assigned by the server
		if dm.TransactionManager.IsNewTransactionId(deleteArgs.transactionId) {
			return nil, errors.New("transactionId not allowed")
		} else {
			deleteArgs.isPartOfExistingTransaction = true
		}
	}

	if argLen > 2 {
		return nil, errors.New("command must have at most 2 arguments - key, transactionId")
	}

	return deleteArgs, nil
}

func execDelete(dm *dbmanager.DBManager, deleteArgs *DeleteArgs, ctx *CommandContext) ([]byte, error) {
	key := deleteArgs.key
	isPartOfExistingTransaction := deleteArgs.isPartOfExistingTransaction
	transactionId := deleteArgs.transactionId

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
	
	gsn := dm.GsnManager.GetNewGsn()

	keyObj := &common.K{Key: key, Gsn: gsn}

	// Get old value using the correct read order (transaction store first, then buffer store)
	oldValue, err := dm.TransactionManager.ReadValue(transactionId, key, dm.StoreManager.BufferStore, ctx.clientConnection)
	if err != nil {
		dm.TransactionManager.ClearTransactionStore(transactionId)
		return nil, err
	}

	tombstone := &common.V{Type: common.TypeTombstone, Value: nil}

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

	transactionRow := common.NewTransactionRow(transactionId, common.DB_OP_DELETE, transactionState, keyObj, oldValue, tombstone)

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
