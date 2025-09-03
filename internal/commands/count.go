package commands

import (
	"errors"
	"meteor/internal/common"
	"meteor/internal/dbmanager"
	"strconv"
	"strings"
)

func init() {
	Register("COUNT", []ArgSpec{
		{Name: "condition", Type: "string", Required: false, Description: "Optional WHERE condition for counting (e.g., 'WHERE status = active')"},
		{Name: "transactionId", Type: "uint32", Required: false, Description: "The transaction id for the count operation"},
	}, ensureCount, execCount)
}

type CountArgs struct {
	condition                   string
	hasCondition                bool
	isPartOfExistingTransaction bool
	transactionId               uint32
}

func ensureCount(dm *dbmanager.DBManager, cmd *common.Command) (*CountArgs, error) {
	argLen := len(cmd.Args)
	if argLen > 2 {
		return nil, errors.New("command must have at most 2 arguments - condition, transactionId")
	}

	countArgs := &CountArgs{
		condition:                   "",
		hasCondition:                false,
		isPartOfExistingTransaction: false,
		transactionId:               0,
	}

	// Parse arguments - could be no args, condition only, transactionId only, or both
	for i, arg := range cmd.Args {
		if strings.HasPrefix(arg, "WHERE ") {
			countArgs.condition = arg
			countArgs.hasCondition = true
		} else {
			// Try to parse as transactionId
			transactionId64Bits, err := strconv.ParseUint(arg, 10, 32)
			if err != nil {
				if i == 0 && !countArgs.hasCondition {
					// First argument is not a transaction ID and doesn't start with WHERE
					return nil, errors.New("first argument must be a WHERE condition or transaction ID")
				}
				return nil, errors.New("invalid transactionId: " + arg)
			}
			countArgs.transactionId = uint32(transactionId64Bits)

			if dm.TransactionManager.IsNewTransactionId(countArgs.transactionId) {
				return nil, errors.New("transactionId not allowed")
			} else {
				countArgs.isPartOfExistingTransaction = true
			}
		}
	}

	// If no transactionId was provided, get a new one
	if countArgs.transactionId == 0 {
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

	if countArgs.hasCondition {
		// Acquire predicate lock for the condition to prevent phantom reads
		err = dm.TransactionManager.AcquirePredicateLock(transactionId, countArgs.condition)
		if err != nil {
			dm.TransactionManager.ClearTransactionStore(transactionId)
			return nil, err
		}
	} else {
		// For full table count, acquire a global predicate lock to prevent modifications
		globalPredicate := "COUNT(*)"
		err = dm.TransactionManager.AcquirePredicateLock(transactionId, globalPredicate)
		if err != nil {
			dm.TransactionManager.ClearTransactionStore(transactionId)
			return nil, err
		}
	}

	var count int

	var results map[string]*common.V

	if countArgs.hasCondition {
		// Count with condition
		filterFunc, err := parseCountCondition(countArgs.condition)
		if err != nil {
			dm.TransactionManager.ClearTransactionStore(transactionId)
			return nil, err
		}
		// TODO: Implement this separately to only count instead of getting all elements to save memory
		results, err = dm.TransactionManager.ReadFilteredValues(transactionId, filterFunc, dm.StoreManager.BufferStore, ctx.clientConnection)
		if err != nil {
			dm.TransactionManager.ClearTransactionStore(transactionId)
			return nil, err
		}
	} else {
		filterFunc := func(key string, value *common.V) bool {
			return value.Type != common.TypeTombstone
		}
		// TODO: Implement this separately to only count instead of getting all elements to save memory
		results, err = dm.TransactionManager.ReadFilteredValues(transactionId, filterFunc, dm.StoreManager.BufferStore, ctx.clientConnection)
		if err != nil {
			dm.TransactionManager.ClearTransactionStore(transactionId)
			return nil, err
		}
	}

	// Process read values for transaction store and read locks
	err = addReadValuesToTxnStoreByAcquiringLocks(dm, transactionId, results, isolationLevel, ctx.clientConnection)
	if err != nil {
		dm.TransactionManager.ClearTransactionStore(transactionId)
		return nil, err
	}

	count = len(results)

	return []byte(strconv.Itoa(count)), nil
}

// parseCountCondition parses WHERE conditions for count operations
func parseCountCondition(condition string) (func(string, *common.V) bool, error) {
	// Remove "WHERE " prefix
	expression := strings.TrimPrefix(condition, "WHERE ")

	// Parse simple conditions
	parts := strings.Fields(expression)
	if len(parts) < 3 {
		return nil, errors.New("invalid condition expression format")
	}

	field := parts[0]
	operator := parts[1]
	operand := strings.Join(parts[2:], " ") // Handle multi-word operands

	// Remove quotes if present
	if strings.HasPrefix(operand, "'") && strings.HasSuffix(operand, "'") {
		operand = operand[1 : len(operand)-1]
	}

	switch field {
	case "key", "key_name":
		return createCountKeyFilter(operator, operand)
	case "key_prefix":
		return createCountKeyPrefixFilter(operator, operand)
	case "$value", "value":
		return createCountValueFilter(operator, operand)
	default:
		return nil, errors.New("unsupported field in condition: " + field)
	}
}

func createCountKeyFilter(operator, operand string) (func(string, *common.V) bool, error) {
	return func(key string, value *common.V) bool {
		if value.Type == common.TypeTombstone {
			return false
		}

		switch operator {
		case "=", "==":
			return key == operand
		case "!=":
			return key != operand
		case "LIKE":
			// Simple LIKE implementation with % wildcards
			if strings.HasSuffix(operand, "%") {
				prefix := strings.TrimSuffix(operand, "%")
				return strings.HasPrefix(key, prefix)
			}
			if strings.HasPrefix(operand, "%") {
				suffix := strings.TrimPrefix(operand, "%")
				return strings.HasSuffix(key, suffix)
			}
			return strings.Contains(key, operand)
		case ">":
			return key > operand
		case "<":
			return key < operand
		case ">=":
			return key >= operand
		case "<=":
			return key <= operand
		default:
			return false
		}
	}, nil
}

func createCountKeyPrefixFilter(operator, operand string) (func(string, *common.V) bool, error) {
	if operator != "=" && operator != "==" {
		return nil, errors.New("key_prefix only supports = operator")
	}

	return func(key string, value *common.V) bool {
		if value.Type == common.TypeTombstone {
			return false
		}
		return strings.HasPrefix(key, operand)
	}, nil
}

func createCountValueFilter(operator, operand string) (func(string, *common.V) bool, error) {
	return func(key string, value *common.V) bool {
		if value.Type == common.TypeTombstone {
			return false
		}

		valueStr := string(value.Value)

		switch operator {
		case "=", "==":
			return valueStr == operand
		case "!=":
			return valueStr != operand
		case ">":
			// Try numeric comparison first
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
		case ">=":
			if valueNum, err := strconv.ParseFloat(valueStr, 64); err == nil {
				if operandNum, err := strconv.ParseFloat(operand, 64); err == nil {
					return valueNum >= operandNum
				}
			}
			return valueStr >= operand
		case "<=":
			if valueNum, err := strconv.ParseFloat(valueStr, 64); err == nil {
				if operandNum, err := strconv.ParseFloat(operand, 64); err == nil {
					return valueNum <= operandNum
				}
			}
			return valueStr <= operand
		case "LIKE":
			return strings.Contains(valueStr, operand)
		default:
			return false
		}
	}, nil
}
