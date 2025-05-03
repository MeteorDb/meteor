package common

type TransactionPayload struct {
	Key K
	OldValue V
	NewValue V
}

type TransactionRow struct {
	TransactionId uint32
	Operation string
	Payload TransactionPayload
}

func NewTransactionRow(transactionId uint32, operation string, key K, oldValue V, newValue V) *TransactionRow {
	return &TransactionRow{
		TransactionId: transactionId,
		Operation: operation,
		Payload: TransactionPayload{
			Key: key,
			OldValue: oldValue,
			NewValue: newValue,
		},
	}
}
