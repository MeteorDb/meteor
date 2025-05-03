package transactionmanager

import (
	"meteor/internal/common"
	"sync/atomic"
)

var transactionId atomic.Uint32

type TransactionManager struct {
	transactions map[uint32]*common.TransactionRow
}

func NewTransactionManager() (*TransactionManager, error) {
	return &TransactionManager{
		transactions: make(map[uint32]*common.TransactionRow),
	}, nil
}

func GetNewTransactionId() uint32 {
	return transactionId.Add(1)
}

func (tm *TransactionManager) AddTransaction(transactionRow *common.TransactionRow) {
	tm.transactions[transactionRow.TransactionId] = transactionRow
}
