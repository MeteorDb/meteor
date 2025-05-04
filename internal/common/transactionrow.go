package common

import "fmt"

type TransactionPayload struct {
	Key *K
	OldValue *V
	NewValue *V
}

func (p *TransactionPayload) String() string {
	return fmt.Sprintf("{Key: %v, OldValue: %v, NewValue: %v}", p.Key, p.OldValue, p.NewValue)
}

type TransactionRow struct {
	TransactionId uint32
	Operation string
	State string
	Payload *TransactionPayload
}

func (t *TransactionRow) String() string {
	return fmt.Sprintf("{TransactionId: %d, Operation: %s, State: %s, Payload: %v}", t.TransactionId, t.Operation, t.State, t.Payload)
}

func NewTransactionRow(transactionId uint32, operation string, state string, key *K, oldValue *V, newValue *V) *TransactionRow {
	return &TransactionRow{
		TransactionId: transactionId,
		Operation: operation,
		State: state,
		Payload: &TransactionPayload{
			Key: key,
			OldValue: oldValue,
			NewValue: newValue,
		},
	}
}
