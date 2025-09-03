package commands

import (
	"encoding/json"
	"errors"
	"fmt"
	"meteor/internal/common"
	"meteor/internal/dbmanager"
	"strconv"
	"strings"
)

func init() {
	Register("SCAN", []ArgSpec{
		{Name: "pattern", Type: "string", Required: true, Description: "The scan pattern (prefix_* or key range)"},
		{Name: "filter", Type: "string", Required: false, Description: "Optional filter expression (WHERE $value > 100)"},
		{Name: "transactionId", Type: "uint32", Required: false, Description: "The transaction id for the scan operation"},
	}, ensureScan, execScan)
}

type ScanArgs struct {
	pattern                     string
	filter                      string
	hasFilter                   bool
	isPartOfExistingTransaction bool
	transactionId               uint32
}

func ensureScan(dm *dbmanager.DBManager, cmd *common.Command) (*ScanArgs, error) {
	argLen := len(cmd.Args)
	if argLen < 1 {
		return nil, errors.New("command must have at least one argument - pattern")
	}
	if argLen > 3 {
		return nil, errors.New("command must have at most 3 arguments - pattern, filter, transactionId")
	}

	scanArgs := &ScanArgs{
		pattern:                     cmd.Args[0],
		filter:                      "",
		hasFilter:                   false,
		isPartOfExistingTransaction: false,
		transactionId:               0,
	}

	// Parse optional filter argument
	if argLen >= 2 && strings.HasPrefix(cmd.Args[1], "WHERE") {
		scanArgs.filter = cmd.Args[1]
		scanArgs.hasFilter = true

		// If we have a filter and a third argument, it should be transactionId
		if argLen == 3 {
			transactionId64Bits, err := strconv.ParseUint(cmd.Args[2], 10, 32)
			if err != nil {
				return nil, errors.New("invalid transactionId")
			}
			scanArgs.transactionId = uint32(transactionId64Bits)
			if !dm.TransactionManager.IsNewTransactionId(scanArgs.transactionId) {
				scanArgs.isPartOfExistingTransaction = true
			} else {
				return nil, errors.New("transactionId not allowed")
			}
		} else {
			scanArgs.transactionId = dm.TransactionManager.GetNewTransactionId()
		}
	} else if argLen == 2 {
		// Second argument is transactionId (no filter)
		transactionId64Bits, err := strconv.ParseUint(cmd.Args[1], 10, 32)
		if err != nil {
			return nil, errors.New("invalid transactionId")
		}
		scanArgs.transactionId = uint32(transactionId64Bits)
		if !dm.TransactionManager.IsNewTransactionId(scanArgs.transactionId) {
			scanArgs.isPartOfExistingTransaction = true
		} else {
			return nil, errors.New("transactionId not allowed")
		}
	} else {
		// No transactionId provided
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

	if strings.HasSuffix(scanArgs.pattern, "*") {
		// For prefix scans, acquire range lock for the prefix range
		prefix := strings.TrimSuffix(scanArgs.pattern, "*")
		// Lock from prefix to prefix + "\xFF" to cover all keys with the prefix
		endKey := prefix + "\xFF"
		err = dm.TransactionManager.AcquireRangeLock(transactionId, prefix, endKey)
		if err != nil {
			dm.TransactionManager.ClearTransactionStore(transactionId)
			return nil, err
		}
	} else {
		// For pattern-based scans, acquire predicate lock
		predicate := fmt.Sprintf("key LIKE '%s'", scanArgs.pattern)
		err = dm.TransactionManager.AcquirePredicateLock(transactionId, predicate)
		if err != nil {
			dm.TransactionManager.ClearTransactionStore(transactionId)
			return nil, err
		}
	}

	// If there's an additional filter, acquire predicate lock for that too
	if scanArgs.hasFilter {
		err = dm.TransactionManager.AcquirePredicateLock(transactionId, scanArgs.filter)
		if err != nil {
			dm.TransactionManager.ClearTransactionStore(transactionId)
			return nil, err
		}
	}

	var results map[string]*common.V

	// Determine scan type and execute
	if strings.HasSuffix(scanArgs.pattern, "*") {
		// Prefix scan
		prefix := strings.TrimSuffix(scanArgs.pattern, "*")
		results, err = dm.TransactionManager.ReadPrefixValues(transactionId, prefix, dm.StoreManager.BufferStore, ctx.clientConnection)
	} else {
		// Treat as full scan with pattern matching (could be extended for other patterns)
		filterFunc := func(key string, value *common.V) bool {
			// Simple pattern matching - can be extended for more complex patterns
			return strings.Contains(key, scanArgs.pattern)
		}
		results, err = dm.TransactionManager.ReadFilteredValues(transactionId, filterFunc, dm.StoreManager.BufferStore, ctx.clientConnection)
	}

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

	// Apply filter if provided
	if scanArgs.hasFilter {
		filteredResults := make(map[string]*common.V)
		filterFunc, err := parseFilterExpression(scanArgs.filter)
		if err != nil {
			dm.TransactionManager.ClearTransactionStore(transactionId)
			return nil, err
		}

		for key, value := range results {
			if filterFunc(key, value) {
				filteredResults[key] = value
			}
		}
		results = filteredResults
	}

	// Convert results to JSON
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

// parseFilterExpression parses simple filter expressions like "WHERE $value > 100"
func parseFilterExpression(filter string) (func(string, *common.V) bool, error) {
	// Remove "WHERE " prefix
	if !strings.HasPrefix(filter, "WHERE ") {
		return nil, errors.New("filter must start with 'WHERE '")
	}

	expression := strings.TrimPrefix(filter, "WHERE ")

	// For now, support simple $value comparisons
	if strings.Contains(expression, "$value") {
		return parseValueFilter(expression)
	}

	// Support key-based filters (treating the key as the field name)
	return parseKeyFilter(expression)
}

func parseValueFilter(expression string) (func(string, *common.V) bool, error) {
	// Parse expressions like "$value > 100", "$value = 'text'"
	parts := strings.Fields(expression)
	if len(parts) != 3 {
		return nil, errors.New("invalid filter expression format")
	}

	if parts[0] != "$value" {
		return nil, errors.New("value filter must start with $value")
	}

	operator := parts[1]
	operand := parts[2]

	return func(key string, value *common.V) bool {
		if value.Type == common.TypeTombstone {
			return false
		}

		valueStr := string(value.Value)

		switch operator {
		case ">":
			// Try numeric comparison
			if valueNum, err := strconv.ParseFloat(valueStr, 64); err == nil {
				if operandNum, err := strconv.ParseFloat(operand, 64); err == nil {
					return valueNum > operandNum
				}
			}
			// Fall back to string comparison
			return valueStr > operand
		case "<":
			if valueNum, err := strconv.ParseFloat(valueStr, 64); err == nil {
				if operandNum, err := strconv.ParseFloat(operand, 64); err == nil {
					return valueNum < operandNum
				}
			}
			return valueStr < operand
		case "=", "==":
			// Remove quotes if present
			if strings.HasPrefix(operand, "'") && strings.HasSuffix(operand, "'") {
				operand = operand[1 : len(operand)-1]
			}
			return valueStr == operand
		case "!=":
			if strings.HasPrefix(operand, "'") && strings.HasSuffix(operand, "'") {
				operand = operand[1 : len(operand)-1]
			}
			return valueStr != operand
		default:
			return false
		}
	}, nil
}

func parseKeyFilter(expression string) (func(string, *common.V) bool, error) {
	// For key-based filters, we can implement more complex logic later
	// For now, return a simple filter that always returns true
	return func(key string, value *common.V) bool {
		return value.Type != common.TypeTombstone
	}, nil
}
