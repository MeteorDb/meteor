package commands

import (
	"errors"
	"meteor/internal/common"
	"meteor/internal/dbmanager"
	"strconv"
)

func init() {
	Register("BEGIN", []ArgSpec{}, ensureBegin, execBegin)
}

type BeginArgs struct {}

func ensureBegin(dm *dbmanager.DBManager, cmd *common.Command) (*BeginArgs, error) {
	if len(cmd.Args) != 0 {
		return nil, errors.New("command must have no arguments")
	}

	beginArgs := &BeginArgs{}

	return beginArgs, nil
}

func execBegin(dm *dbmanager.DBManager, beginArgs *BeginArgs, ctx *CommandContext) ([]byte, error) {
	transactionId := dm.TransactionManager.GetNewTransactionId()

	gsn := dm.GsnManager.GetNewGsn()
	key := &common.K{Key: common.TypeKeyNull, Gsn: gsn}

	transactionRow := common.NewTransactionRow(transactionId, common.DB_OP_BEGIN, common.TRANSACTION_STATE_QUEUED, key, nil, nil)

	err := dm.TransactionManager.AddTransaction(transactionRow, ctx.clientConnection)
	if err != nil {
		return nil, err
	}

	err = dm.AddTransactionToWal(transactionRow)
	if err != nil {
		return nil, err
	}

	return []byte(strconv.FormatUint(uint64(transactionId), 10)), nil
}
