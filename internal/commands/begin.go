package commands

import (
	"errors"
	"meteor/internal/common"
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
		transactionIsolation: common.TXN_ISOLATION_READ_COMMITTED,
	}

	if argLen == 1 {
		transactionIsolation := cmd.Args[0]
		validIsolationLevels := []string{
			common.TXN_ISOLATION_READ_COMMITTED,
			common.TXN_ISOLATION_REPEATABLE_READ,
			common.TXN_ISOLATION_SNAPSHOT_ISOLATION,
			common.TXN_ISOLATION_SERIALIZABLE,
		}
		
		isValid := false
		for _, level := range validIsolationLevels {
			if transactionIsolation == level {
				isValid = true
				break
			}
		}
		
		if !isValid {
			return nil, errors.New("invalid transaction isolation level. Valid levels are: READ_COMMITTED, REPEATABLE_READ, SNAPSHOT_ISOLATION, SERIALIZABLE")
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
	
	// Set transaction start GSN
	// Required only for SNAPSHOT_ISOLATION
	if beginArgs.transactionIsolation == common.TXN_ISOLATION_SNAPSHOT_ISOLATION {
		dm.TransactionManager.SetTransactionStartGsn(transactionId, gsn)
	}
	key := &common.K{Key: common.TypeKeyNull, Gsn: gsn}

	transactionRow := common.NewTransactionRow(transactionId, common.DB_OP_BEGIN, common.TRANSACTION_STATE_QUEUED, key, nil, nil)

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

	return []byte(strconv.FormatUint(uint64(transactionId), 10)), nil
}
