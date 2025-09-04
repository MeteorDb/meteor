package commands

import (
	"errors"
	"fmt"
	"meteor/internal/common"
	"meteor/internal/dbmanager"
	"meteor/internal/parser"
	"strconv"
)

func init() {
	Register("COUNT", []ArgSpec{
		{Name: "condition", Type: "string", Required: true, Description: "Condition for counting (e.g., '$key LIKE user_%' or '$value > 100' or '$key = user1 AND $value > 50' or '*' for all records)"},
		{Name: "transactionId", Type: "uint32", Required: false, Description: "The transaction id for the count operation"},
	}, ensureCount, execCount)
}

type CountArgs struct {
	condition                   string
	isPartOfExistingTransaction bool
	transactionId               uint32
}

func ensureCount(dm *dbmanager.DBManager, cmd *common.Command) (*CountArgs, error) {
	argLen := len(cmd.Args)
	if argLen < 1 {
		return nil, errors.New("command must have at least one argument - condition")
	}
	if argLen > 2 {
		return nil, errors.New("command must have at most 2 arguments - condition, transactionId")
	}

	countArgs := &CountArgs{
		condition:                   cmd.Args[0],
		isPartOfExistingTransaction: false,
		transactionId:               0,
	}

	// Handle optional transactionId argument
	if argLen == 2 {
		transactionId64Bits, err := strconv.ParseUint(cmd.Args[1], 10, 32)
		if err != nil {
			return nil, errors.New("invalid transactionId")
		}
		countArgs.transactionId = uint32(transactionId64Bits)

		if dm.TransactionManager.IsNewTransactionId(countArgs.transactionId) {
			return nil, errors.New("transactionId not allowed")
		} else {
			countArgs.isPartOfExistingTransaction = true
		}
	} else {
		countArgs.transactionId = dm.TransactionManager.GetNewTransactionId()
	}

	return countArgs, nil
}

func execCount(dm *dbmanager.DBManager, countArgs *CountArgs, ctx *CommandContext) ([]byte, error) {
	transactionId := countArgs.transactionId
	isolationLevel, err := dm.TransactionManager.GetIsolationLevel(transactionId)
	if err != nil {
		return nil, err
	}

	// Determine predicate for locking
	predicate := countArgs.condition
	if countArgs.condition == "*" {
		predicate = "COUNT(*)"
	}

	// Acquire predicate lock for the condition to prevent phantom reads
	err = dm.TransactionManager.AcquirePredicateLock(transactionId, predicate)
	if err != nil {
		dm.TransactionManager.ClearTransactionStore(transactionId)
		return nil, err
	}

	// Create appropriate filter function
	var filterFunc func(string, *common.V) bool
	if countArgs.condition == "*" {
		// Full count - count all non-tombstone records
		filterFunc = func(key string, value *common.V) bool {
			return value != nil && value.Type != common.TypeTombstone
		}
	} else {
		// Parse the condition using the same parser as SCAN
		conditionParser := parser.NewConditionParser(countArgs.condition)
		var parseErr error
		filterFunc, parseErr = conditionParser.ParseExpression()
		if parseErr != nil {
			dm.TransactionManager.ClearTransactionStore(transactionId)
			return nil, fmt.Errorf("invalid condition: %v", parseErr)
		}
	}

	// TODO: Refactor this to get only count and not all rows to save memory
	results, err := dm.TransactionManager.ReadFilteredValues(transactionId, filterFunc, dm.StoreManager.BufferStore, ctx.clientConnection)
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

	// Count only non-tombstone records
	count := 0
	for _, value := range results {
		if value != nil && value.Type != common.TypeTombstone {
			count++
		}
	}

	return []byte(strconv.Itoa(count)), nil
}

