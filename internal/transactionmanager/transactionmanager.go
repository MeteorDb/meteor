package transactionmanager

import (
	"meteor/internal/common"
	"meteor/internal/walmanager"
	"sync"
	"sync/atomic"
)

type TransactionManager struct {
	walManager *walmanager.WalManager
	transactions map[uint32]*common.TransactionRow
	currentTransactionId atomic.Uint32
	transactionIdBatchStart uint32
	transactionIdBatchEnd uint32
	m sync.Mutex
}

func NewTransactionManager(walManager *walmanager.WalManager) (*TransactionManager, error) {
	transactionIdBatchStart, transactionIdBatchEnd := walManager.AllocateTransactionIdBatch()
	transactionManager := &TransactionManager{
		walManager: walManager,
		transactions: make(map[uint32]*common.TransactionRow),
		currentTransactionId: atomic.Uint32{},
		transactionIdBatchStart: transactionIdBatchStart,
		transactionIdBatchEnd: transactionIdBatchEnd,
		m: sync.Mutex{},
	}

	transactionManager.currentTransactionId.Store(transactionIdBatchStart)

	return transactionManager, nil
}

func (tm *TransactionManager) GetNewTransactionId() uint32 {
	if tm.currentTransactionId.Load() == tm.transactionIdBatchEnd - 1 {
		tm.m.Lock()
		if tm.currentTransactionId.Load() == tm.transactionIdBatchEnd - 1 {
			tm.transactionIdBatchStart, tm.transactionIdBatchEnd = tm.walManager.AllocateTransactionIdBatch()
			tm.currentTransactionId.Store(tm.transactionIdBatchStart)
		}
		tm.m.Unlock()
	}
	return tm.currentTransactionId.Add(1)
}

func (tm *TransactionManager) AddTransaction(transactionRow *common.TransactionRow) {
	tm.transactions[transactionRow.TransactionId] = transactionRow
}
