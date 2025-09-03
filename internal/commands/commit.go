package commands

import (
	"errors"
	"meteor/internal/common"
	"meteor/internal/dbmanager"
	"strconv"
)

func init() {
	Register("COMMIT", []ArgSpec{
		{Name: "transactionId", Type: "uint32", Required: true, Description: "The transaction id to commit"},
	}, ensureCommit, execCommit)
}

type CommitArgs struct {
	transactionId uint32
}

func ensureCommit(dm *dbmanager.DBManager, cmd *common.Command) (*CommitArgs, error) {
	if len(cmd.Args) != 1 {
		return nil, errors.New("command must have one argument - transactionId")
	}

	transactionId64Bits, err := strconv.ParseUint(cmd.Args[0], 10, 32)
	if err != nil {
		return nil, errors.New("invalid transactionId")
	}
	transactionId := uint32(transactionId64Bits)

	return &CommitArgs{transactionId: transactionId}, nil
}

func execCommit(dm *dbmanager.DBManager, commitArgs *CommitArgs, ctx *CommandContext) ([]byte, error) {
	transactionId := commitArgs.transactionId

	gsn := dm.GsnManager.GetNewGsn()
	key := &common.K{Key: common.TypeKeyNull, Gsn: gsn}

	// getting the store before adding transactionRow so store is not created only for this row
	transactionStore := dm.TransactionManager.GetTransactionStore(transactionId)
	if transactionStore == nil {
		return nil, errors.New("transaction not found")
	}

	type validatedEntry struct {
		key       *common.K
		value     *common.V
	}

	var validatedEntries []validatedEntry

	// Apply changes from transaction store to buffer store with conflict detection
	for _, keyStr := range transactionStore.Keys() {
		value := transactionStore.Get(keyStr)
		if value == nil {
			continue
		}

		// Skip GET operations - they shouldn't be applied to buffer store
		// GET operations are identified by having no changes to commit
		// We can determine this by checking if the value is the same as buffer store
		// TODO: Should be a better way to handle this
		bufferValue := dm.StoreManager.BufferStore.Get(keyStr)
		if bufferValue != nil && value.Type == bufferValue.Type && 
		   string(value.Value) == string(bufferValue.Value) {
			continue // This is likely a GET operation, skip
		}

		latestGsn, err := transactionStore.GetLatestGsn(keyStr)
		if err != nil {
			dm.TransactionManager.ClearTransactionStore(transactionId)
			return nil, err
		}

		// Compare buffer store latest GSN with transaction store GSN
		// If buffer store has newer version, detect conflict
		// TODO: We can reuse the bufferValue from above since it returns the value with latest GSN. Refer mapdatatable.go for implementation.
		bufferLatestGsn, bufferErr := dm.StoreManager.BufferStore.GetLatestGsn(keyStr)
		if bufferErr != nil {
			dm.TransactionManager.ClearTransactionStore(transactionId)
			return nil, bufferErr
		}
		// TODO: Ideally this check should be moved to ValidateWrite method. Currently similar check is present in ValidateWrite method but only for SNAPSHOT_ISOLATION. But ValidateWrite is used in other places so need to be careful.
		if bufferLatestGsn > latestGsn {
			// Another transaction has committed after this transaction read the value
			// Clean up and return error
			dm.TransactionManager.ClearTransactionStore(transactionId)
			return nil, errors.New("write-write conflict detected - another transaction committed first for key: " + keyStr)
		}

		// Final validation based on isolation level
		err = dm.TransactionManager.ValidateWrite(transactionId, keyStr, dm.StoreManager.BufferStore, ctx.clientConnection)
		if err != nil {
			// Clean up and return error
			dm.TransactionManager.ClearTransactionStore(transactionId)
			return nil, err
		}

		validatedEntries = append(validatedEntries, validatedEntry{
			key:       &common.K{Key: keyStr, Gsn: latestGsn},
			value:     value,
		})
	}

	// All validations passed, now add transaction row to WAL
	transactionRow := common.NewTransactionRow(transactionId, common.DB_OP_COMMIT, common.TRANSACTION_STATE_COMMIT, key, nil, nil)

	err := dm.TransactionManager.AddTransaction(transactionRow, ctx.clientConnection)
	if err != nil {
		dm.TransactionManager.ClearTransactionStore(transactionId)
		return nil, err
	}

	err = dm.AddTransactionToWal(transactionRow)
	if err != nil {
		dm.TransactionManager.ClearTransactionStore(transactionId)
		return nil, err
	}

	// Apply all validated entries to buffer store
	for _, entry := range validatedEntries {
		err = dm.StoreManager.BufferStore.Put(entry.key, entry.value)
		if err != nil {
			// Clean up and return error
			dm.TransactionManager.ClearTransactionStore(transactionId)
			return nil, err
		}
	}

	// Clean up transaction resources (this includes lock cleanup)
	dm.TransactionManager.ClearTransactionStore(transactionId)

	return []byte("OK"), nil
}
