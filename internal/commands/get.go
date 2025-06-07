package commands

import (
	"errors"
	"meteor/internal/common"
	"meteor/internal/dbmanager"
	"strconv"
)

func init() {
	Register("GET", []ArgSpec{
		{ Name: "key", Type: "string", Required: true, Description: "The key to get" },
		{ Name: "transactionId", Type: "uint32", Required: false, Description: "The transaction id to get the key from" },
	}, ensureGet, execGet)
}

type GetArgs struct {
	key string
	isPartOfExistingTransaction bool
	transactionId uint32
}

func ensureGet(dm *dbmanager.DBManager, cmd *common.Command) (*GetArgs, error) {
	if len(cmd.Args) < 1 {
		return nil, errors.New("command must have at least one argument - key")
	}

	getArgs := &GetArgs{
		key: cmd.Args[0],
		isPartOfExistingTransaction: false,
		transactionId: 0,
	}

	if len(cmd.Args) == 2 {
		transactionId64Bits, err := strconv.ParseUint(cmd.Args[1], 10, 32)
		if err != nil {
			return nil, errors.New("invalid transactionId")
		}
		transactionId := uint32(transactionId64Bits)
		getArgs.transactionId = transactionId
		getArgs.isPartOfExistingTransaction = true
	}

	if len(cmd.Args) > 2 {
		return nil, errors.New("command must have at most two arguments - key, transactionId")
	}

	return getArgs, nil
}

func execGet(dm *dbmanager.DBManager, getArgs *GetArgs, ctx *CommandContext) ([]byte, error) {
	key := getArgs.key
	isPartOfExistingTransaction := getArgs.isPartOfExistingTransaction
	transactionId := getArgs.transactionId

	if isPartOfExistingTransaction {
		// if part of existing transaction, then search the transaction store first
		store, err := dm.TransactionManager.GetStoreByTransactionId(transactionId, ctx.clientConnection)
		if err != nil {
			return nil, err
		}

		if store != nil {
			v := store.Get(key)
			if v != nil {
				return v.Value, nil
			}
		}
	}

	v := dm.StoreManager.BufferStore.Get(key)

	if v == nil {
		return []byte("-1"), nil
	}

	if v.Type == common.TypeTombstone {
		return []byte("-2"), nil
	}

	return v.Value, nil
}
