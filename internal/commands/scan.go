package commands

import (
	"encoding/json"
	"errors"
	"fmt"
	"meteor/internal/common"
	"meteor/internal/dbmanager"
	"meteor/internal/parser"
	"strconv"
)

func init() {
	Register("SCAN", []ArgSpec{
		{Name: "condition", Type: "string", Required: true, Description: "Condition for filtering (e.g., '$key LIKE user_%' or '$value > 100' or '$key = user1 AND $value > 50' or '*' for all records)"},
		{Name: "transactionId", Type: "uint32", Required: false, Description: "The transaction id for the scan operation"},
	}, ensureScan, execScan)
}

type ScanArgs struct {
	condition                   string
	isPartOfExistingTransaction bool
	transactionId               uint32
}

func ensureScan(dm *dbmanager.DBManager, cmd *common.Command) (*ScanArgs, error) {
	argLen := len(cmd.Args)
	if argLen < 1 {
		return nil, errors.New("command must have at least one argument - condition")
	}
	if argLen > 2 {
		return nil, errors.New("command must have at most 2 arguments - condition, transactionId")
	}

	scanArgs := &ScanArgs{
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
		scanArgs.transactionId = uint32(transactionId64Bits)

		if dm.TransactionManager.IsNewTransactionId(scanArgs.transactionId) {
			return nil, errors.New("transactionId not allowed")
		} else {
			scanArgs.isPartOfExistingTransaction = true
		}
	} else {
		scanArgs.transactionId = dm.TransactionManager.GetNewTransactionId()
	}

	return scanArgs, nil
}

func execScan(dm *dbmanager.DBManager, scanArgs *ScanArgs, ctx *CommandContext) ([]byte, error) {
	transactionId := scanArgs.transactionId
	isolationLevel, err := dm.TransactionManager.GetIsolationLevel(transactionId)
	if err != nil {
		return nil, err
	}

	// Determine predicate for locking
	predicate := scanArgs.condition
	if scanArgs.condition == "*" {
		predicate = "SCAN(*)"
	}

	// Acquire predicate lock for the condition to prevent phantom reads
	err = dm.TransactionManager.AcquirePredicateLock(transactionId, predicate)
	if err != nil {
		dm.TransactionManager.ClearTransactionStore(transactionId)
		return nil, err
	}

	// Create appropriate filter function
	var filterFunc func(string, *common.V) bool
	if scanArgs.condition == "*" {
		// Full scan - return all non-tombstone records
		filterFunc = func(key string, value *common.V) bool {
			return value != nil && value.Type != common.TypeTombstone
		}
	} else {
		// Parse the condition into a filter function using the new parser
		conditionParser := parser.NewConditionParser(scanArgs.condition)
		var parseErr error
		filterFunc, parseErr = conditionParser.ParseExpression()
		if parseErr != nil {
			dm.TransactionManager.ClearTransactionStore(transactionId)
			return nil, fmt.Errorf("invalid condition: %v", parseErr)
		}
	}

	// Execute scan using transaction-aware read
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

	// Convert results to JSON, excluding tombstones
	jsonResults := make(map[string]interface{})
	for key, value := range results {
		if value == nil || value.Type == common.TypeTombstone {
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

