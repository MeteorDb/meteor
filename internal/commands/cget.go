package commands

import (
	"encoding/json"
	"errors"
	"meteor/internal/common"
	"meteor/internal/dbmanager"
	"strconv"
	"strings"
)

func init() {
	Register("CGET", []ArgSpec{
		{Name: "condition", Type: "string", Required: true, Description: "The WHERE condition for key matching (e.g., 'WHERE key_prefix = user')"},
		{Name: "transactionId", Type: "uint32", Required: false, Description: "The transaction id for the conditional get operation"},
	}, ensureCget, execCget)
}

type CgetArgs struct {
	condition                   string
	isPartOfExistingTransaction bool
	transactionId               uint32
}

func ensureCget(dm *dbmanager.DBManager, cmd *common.Command) (*CgetArgs, error) {
	argLen := len(cmd.Args)
	if argLen < 1 {
		return nil, errors.New("command must have at least one argument - condition")
	}
	if argLen > 2 {
		return nil, errors.New("command must have at most 2 arguments - condition, transactionId")
	}

	cgetArgs := &CgetArgs{
		condition:                   cmd.Args[0],
		isPartOfExistingTransaction: false,
		transactionId:               0,
	}

	// Validate condition format
	if !strings.HasPrefix(cgetArgs.condition, "WHERE ") {
		return nil, errors.New("condition must start with 'WHERE '")
	}

	// Handle optional transactionId argument
	if argLen == 2 {
		transactionId64Bits, err := strconv.ParseUint(cmd.Args[1], 10, 32)
		if err != nil {
			return nil, errors.New("invalid transactionId")
		}
		cgetArgs.transactionId = uint32(transactionId64Bits)

		if dm.TransactionManager.IsNewTransactionId(cgetArgs.transactionId) {
			return nil, errors.New("transactionId not allowed")
		} else {
			cgetArgs.isPartOfExistingTransaction = true
		}
	} else {
		cgetArgs.transactionId = dm.TransactionManager.GetNewTransactionId()
	}

	return cgetArgs, nil
}

func execCget(dm *dbmanager.DBManager, cgetArgs *CgetArgs, ctx *CommandContext) ([]byte, error) {
	transactionId := cgetArgs.transactionId
	isolationLevel, err := dm.TransactionManager.GetIsolationLevel(transactionId)
	if err != nil {
		return nil, err
	}

	// Acquire predicate lock for the condition to prevent phantom reads
	err = dm.TransactionManager.AcquirePredicateLock(transactionId, cgetArgs.condition)
	if err != nil {
		dm.TransactionManager.ClearTransactionStore(transactionId)
		return nil, err
	}

	// Parse the condition into a filter function
	filterFunc, err := parseConditionExpression(cgetArgs.condition)
	if err != nil {
		dm.TransactionManager.ClearTransactionStore(transactionId)
		return nil, err
	}

	// Execute conditional scan using transaction-aware read
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

// parseConditionExpression parses WHERE conditions for key-based filtering
func parseConditionExpression(condition string) (func(string, *common.V) bool, error) {
	// Remove "WHERE " prefix
	expression := strings.TrimPrefix(condition, "WHERE ")

	// Parse simple key-based conditions like "key_prefix = user", "key LIKE user_%"
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
		return createKeyFilter(operator, operand)
	case "key_prefix":
		return createKeyPrefixFilter(operator, operand)
	default:
		return nil, errors.New("unsupported field in condition: " + field)
	}
}

func createKeyFilter(operator, operand string) (func(string, *common.V) bool, error) {
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
			// Contains
			if strings.Contains(operand, "%") {
				parts := strings.Split(operand, "%")
				if len(parts) == 2 {
					return strings.HasPrefix(key, parts[0]) && strings.HasSuffix(key, parts[1])
				}
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

func createKeyPrefixFilter(operator, operand string) (func(string, *common.V) bool, error) {
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
