package transactionmanager

import (
	"errors"
	"meteor/internal/common"
	"meteor/internal/walmanager"
	"net"
	"slices"
	"sync"
	"sync/atomic"
)

type TransactionManager struct {
	transactionsMap map[uint32][]*common.TransactionRow
	connToTransactionIdsMap map[*net.Conn][]uint32
	walManager *walmanager.WalManager
	currentTransactionId atomic.Uint32
	transactionIdBatchStart uint32
	transactionIdBatchEnd uint32
	m sync.Mutex
}

func NewTransactionManager(walManager *walmanager.WalManager) (*TransactionManager, error) {
	transactionIdBatchStart, transactionIdBatchEnd := walManager.AllocateTransactionIdBatch()
	transactionManager := &TransactionManager{
		walManager: walManager,
		transactionsMap: make(map[uint32][]*common.TransactionRow),
		connToTransactionIdsMap: make(map[*net.Conn][]uint32),
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

func (tm *TransactionManager) AddTransaction(transactionRow *common.TransactionRow, conn *net.Conn) error {
	if !tm.isTransactionIdAllowedForConnection(transactionRow.TransactionId, conn) {
		return errors.New("transaction id not allowed for connection")
	}

	tm.registerTransactionForConnection(transactionRow.TransactionId, conn)

	transactionsArr, ok := tm.transactionsMap[transactionRow.TransactionId]
	if !ok {
		tm.transactionsMap[transactionRow.TransactionId] = make([]*common.TransactionRow, 0)
	}
	tm.transactionsMap[transactionRow.TransactionId] = append(transactionsArr, transactionRow)

	return nil
}

func (tm *TransactionManager) isTransactionIdAllowedForConnection(transactionId uint32, conn *net.Conn) bool {
	isNewTransactionId := tm.isNewTransactionId(transactionId)

	if isNewTransactionId {
		return true
	}

	transactionIds, ok := tm.connToTransactionIdsMap[conn]
	if !ok {
		return false
	}
	return slices.Contains(transactionIds, transactionId)
}

func (tm *TransactionManager) isNewTransactionId(transactionId uint32) bool {
	for tId := range tm.transactionsMap {
		if tId == transactionId {
			return false
		}
	}
	return true
}

func (tm *TransactionManager) registerTransactionForConnection(transactionId uint32, conn *net.Conn) {
	transactionIds, ok := tm.connToTransactionIdsMap[conn]
	if !ok {
		tm.connToTransactionIdsMap[conn] = make([]uint32, 0)
	}

	// should be replaced with a set
	isPresent := false
	for _, tId := range transactionIds {
		if tId == transactionId {
			isPresent = true
			break
		}
	}
	if !isPresent {
		tm.connToTransactionIdsMap[conn] = append(transactionIds, transactionId)
	}
}
