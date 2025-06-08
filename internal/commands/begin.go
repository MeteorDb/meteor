package commands

import (
	"errors"
	"meteor/internal/common"
	"meteor/internal/config"
	"meteor/internal/dbmanager"
	"strconv"
)

func init() {
	Register("BEGIN", []ArgSpec{
		{ Name: "transactionIsolation", Type: "string", Required: false, Description: "The transaction isolation level" },
	}, ensureBegin, execBegin)
}

type BeginArgs struct {
	transactionIsolation string
}

func ensureBegin(dm *dbmanager.DBManager, cmd *common.Command) (*BeginArgs, error) {
	argLen := len(cmd.Args)

	if argLen != 0 && argLen != 1 {
		return nil, errors.New("command must have no arguments or one argument - transactionIsolation")
	}

	beginArgs := &BeginArgs{
		transactionIsolation: config.TXN_ISOLATION_READ_COMMITTED,
	}

	if argLen == 1 {
		transactionIsolation := cmd.Args[0]
		if transactionIsolation != config.TXN_ISOLATION_READ_COMMITTED && transactionIsolation != config.TXN_ISOLATION_REPEATABLE_READ && transactionIsolation != config.TXN_ISOLATION_SERIALIZABLE {
			return nil, errors.New("invalid transaction isolation level")
		}
		beginArgs.transactionIsolation = transactionIsolation
	}

	return beginArgs, nil
}

func execBegin(dm *dbmanager.DBManager, beginArgs *BeginArgs, ctx *CommandContext) ([]byte, error) {
	transactionId := dm.TransactionManager.GetNewTransactionId()

	err := dm.TransactionManager.EnsureIsolationLevel(transactionId, beginArgs.transactionIsolation)
	if err != nil {
		return nil, err
	}
	
	gsn := dm.GsnManager.GetNewGsn()
	key := &common.K{Key: common.TypeKeyNull, Gsn: gsn}

	transactionRow := common.NewTransactionRow(transactionId, common.DB_OP_BEGIN, common.TRANSACTION_STATE_QUEUED, key, nil, nil)

	err = dm.TransactionManager.AddTransaction(transactionRow, ctx.clientConnection)
	if err != nil {
		return nil, err
	}

	err = dm.AddTransactionToWal(transactionRow)
	if err != nil {
		return nil, err
	}

	return []byte(strconv.FormatUint(uint64(transactionId), 10)), nil
}
