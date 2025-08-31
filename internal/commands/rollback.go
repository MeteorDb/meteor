package commands

import (
	"errors"
	"meteor/internal/common"
	"meteor/internal/dbmanager"
	"strconv"
)

func init() {
	Register("ROLLBACK", []ArgSpec{
		{ Name: "transactionId", Type: "uint32", Required: true, Description: "The transaction id to rollback" },
	}, ensureRollback, execRollback)
}

type RollbackArgs struct {
	transactionId uint32
}

func ensureRollback(dm *dbmanager.DBManager, cmd *common.Command) (*RollbackArgs, error) {
	if len(cmd.Args) != 1 {
		return nil, errors.New("command must have one argument - transactionId")
	}
	
	transactionId64Bits, err := strconv.ParseUint(cmd.Args[0], 10, 32)
	if err != nil {
		return nil, errors.New("invalid transactionId")
	}
	transactionId := uint32(transactionId64Bits)

	return &RollbackArgs{transactionId: transactionId}, nil
}

func execRollback(dm *dbmanager.DBManager, rollbackArgs *RollbackArgs, ctx *CommandContext) ([]byte, error) {
	transactionId := rollbackArgs.transactionId

	transactionStore := dm.TransactionManager.GetTransactionStore(transactionId)
	if transactionStore == nil {
		return nil, errors.New("transaction not found")
	}

	gsn := dm.GsnManager.GetNewGsn()
	key := &common.K{Key: common.TypeKeyNull, Gsn: gsn}

	transactionRow := common.NewTransactionRow(transactionId, common.DB_OP_ROLLBACK, common.TRANSACTION_STATE_ROLLBACK, key, nil, nil)

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

	dm.TransactionManager.ClearTransactionStore(transactionId)

	return []byte("OK"), nil
}
