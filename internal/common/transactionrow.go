package common

type TransactionPayload struct {
	Key []byte
	OldValue []byte
	NewValue []byte
}

type TransactionRow struct {
	Gsn uint32
	TransactionId uint32
	Operation string
	Payload TransactionPayload
}

func NewTransactionRow(gsn uint32, transactionId uint32, operation string, key []byte, oldValue []byte, newValue []byte) *TransactionRow {
	return &TransactionRow{
		Gsn: gsn,
		TransactionId: transactionId,
		Operation: operation,
		Payload: TransactionPayload{
			Key: key,
			OldValue: oldValue,
			NewValue: newValue,
		},
	}
}
