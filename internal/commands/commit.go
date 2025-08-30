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

	transactionRow := common.NewTransactionRow(transactionId, common.DB_OP_COMMIT, common.TRANSACTION_STATE_COMMIT, key, nil, nil)

	err := dm.TransactionManager.AddTransaction(transactionRow, ctx.clientConnection)
	if err != nil {
		return nil, err
	}

	err = dm.AddTransactionToWal(transactionRow)
	if err != nil {
		return nil, err
	}

	for _, key := range transactionStore.Keys() {
		latestGsn, err := transactionStore.GetLatestGsn(key)
		if err != nil {
			return nil, err
		}
		// TODO: should only consider PUT/DELETE rows and not GET rows
		// TODO: compare bufferStore latestGsn of the key with the latestGsn of the transactionStore, if the bufferStore latestGsn is greater than the transactionStore latestGsn, then throw error
		dm.StoreManager.BufferStore.Put(&common.K{Key: key, Gsn: latestGsn}, transactionStore.Get(key))
	}

	dm.TransactionManager.ClearTransactionStore(transactionId)

	return []byte("OK"), nil
}
