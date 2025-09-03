package commands

import (
	"encoding/json"
	"errors"
	"meteor/internal/common"
	"meteor/internal/dbmanager"
	"strconv"
)

func init() {
	Register("RGET", []ArgSpec{
		{Name: "startKey", Type: "string", Required: true, Description: "The starting key of the range"},
		{Name: "endKey", Type: "string", Required: true, Description: "The ending key of the range"},
		{Name: "transactionId", Type: "uint32", Required: false, Description: "The transaction id for the range get operation"},
	}, ensureRget, execRget)
}

type RgetArgs struct {
	startKey                    string
	endKey                      string
	isPartOfExistingTransaction bool
	transactionId               uint32
}

func ensureRget(dm *dbmanager.DBManager, cmd *common.Command) (*RgetArgs, error) {
	argLen := len(cmd.Args)
	if argLen < 2 {
		return nil, errors.New("command must have at least two arguments - startKey, endKey")
	}
	if argLen > 3 {
		return nil, errors.New("command must have at most 3 arguments - startKey, endKey, transactionId")
	}

	rgetArgs := &RgetArgs{
		startKey:                    cmd.Args[0],
		endKey:                      cmd.Args[1],
		isPartOfExistingTransaction: false,
		transactionId:               0,
	}

	// Validate that startKey <= endKey lexicographically
	if rgetArgs.startKey > rgetArgs.endKey {
		return nil, errors.New("startKey must be lexicographically less than or equal to endKey")
	}

	// Handle optional transactionId argument
	if argLen == 3 {
		transactionId64Bits, err := strconv.ParseUint(cmd.Args[2], 10, 32)
		if err != nil {
			return nil, errors.New("invalid transactionId")
		}
		rgetArgs.transactionId = uint32(transactionId64Bits)

		if dm.TransactionManager.IsNewTransactionId(rgetArgs.transactionId) {
			return nil, errors.New("transactionId not allowed")
		} else {
			rgetArgs.isPartOfExistingTransaction = true
		}
	} else {
		rgetArgs.transactionId = dm.TransactionManager.GetNewTransactionId()
	}

	return rgetArgs, nil
}

func execRget(dm *dbmanager.DBManager, rgetArgs *RgetArgs, ctx *CommandContext) ([]byte, error) {
	transactionId := rgetArgs.transactionId
	isolationLevel, err := dm.TransactionManager.GetIsolationLevel(transactionId)
	if err != nil {
		return nil, err
	}

	// Acquire range lock for [startKey, endKey] to prevent phantom reads
	err = dm.TransactionManager.AcquireRangeLock(transactionId, rgetArgs.startKey, rgetArgs.endKey)
	if err != nil {
		dm.TransactionManager.ClearTransactionStore(transactionId)
		return nil, err
	}

	results, err := dm.TransactionManager.ReadRangeValues(transactionId, rgetArgs.startKey, rgetArgs.endKey, dm.StoreManager.BufferStore, ctx.clientConnection)
	if err != nil {
		dm.TransactionManager.ClearTransactionStore(transactionId)
		return nil, err
	}

	// Process read values for transaction store and read locks
	err = addReadValuesToTxnStoreByAcquiringLocks(dm, transactionId, results, isolationLevel, ctx.clientConnection)
	if err != nil {
		dm.TransactionManager.ClearTransactionStore(transactionId)
		return nil, err
	}

	// Convert results to JSON, excluding tombstones
	jsonResults := make(map[string]interface{})
	for key, value := range results {
		if value.Type == common.TypeTombstone {
			continue // Skip deleted entries
		}
		jsonResults[key] = string(value.Value)
	}

	jsonBytes, err := json.Marshal(jsonResults)
	if err != nil {
		dm.TransactionManager.ClearTransactionStore(transactionId)
		return nil, err
	}

	return jsonBytes, nil
}
