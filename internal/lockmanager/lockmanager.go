package lockmanager

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// LockType represents the type of lock
type LockType int

const (
	ReadLock LockType = iota
	WriteLock
)

func (lt LockType) String() string {
	switch lt {
	case ReadLock:
		return "READ"
	case WriteLock:
		return "WRITE"
	default:
		return "UNKNOWN"
	}
}

// Lock represents a single lock held by a transaction
type Lock struct {
	TransactionID uint32
	Key           string
	Type          LockType
	AcquiredAt    time.Time
}

// LockRequest represents a request for acquiring a lock
type LockRequest struct {
	TransactionID uint32
	Key           string
	Type          LockType
	AcquiredCh    chan error
}

// LockManager manages locks for transactions
type LockManager struct {
	// lockTable maps key -> list of locks on that key
	lockTable map[string][]*Lock
	// transactionLocks maps transaction ID -> list of locks held by that transaction
	transactionLocks map[uint32][]*Lock
	// waitingRequests maps key -> queue of waiting lock requests
	waitingRequests map[string][]*LockRequest
	// mutex for protecting internal data structures
	mutex sync.RWMutex
}

// NewLockManager creates a new lock manager
func NewLockManager() *LockManager {
	return &LockManager{
		lockTable:        make(map[string][]*Lock),
		transactionLocks: make(map[uint32][]*Lock),
		waitingRequests:  make(map[string][]*LockRequest),
		mutex:            sync.RWMutex{},
	}
}

// AcquireLock attempts to acquire a lock for a transaction
// Returns immediately if lock can be granted, otherwise blocks until available or timeout
func (lm *LockManager) AcquireLock(transactionID uint32, key string, lockType LockType, timeout time.Duration) error {
	lm.mutex.Lock()

	// Check if lock can be granted immediately
	if lm.canGrantLock(key, lockType, transactionID) {
		lock := &Lock{
			TransactionID: transactionID,
			Key:           key,
			Type:          lockType,
			AcquiredAt:    time.Now(),
		}

		lm.grantLock(lock)
		lm.mutex.Unlock()
		return nil
	}

	// Check for deadlock before adding to waiting queue
	if lm.wouldCauseDeadlock(transactionID, key) {
		lm.mutex.Unlock()
		return errors.New("deadlock detected")
	}

	// Add to waiting queue
	request := &LockRequest{
		TransactionID: transactionID,
		Key:           key,
		Type:          lockType,
		AcquiredCh:    make(chan error, 1),
	}

	lm.waitingRequests[key] = append(lm.waitingRequests[key], request)
	lm.mutex.Unlock()

	// Wait for lock to be granted or timeout
	select {
	case err := <-request.AcquiredCh:
		return err
	case <-time.After(timeout):
		// Remove from waiting queue and return timeout error
		lm.removeWaitingRequest(transactionID, key)
		return fmt.Errorf("lock acquisition timeout for transaction %d on key %s", transactionID, key)
	}
}

// TryAcquireLock attempts to acquire a lock without blocking
func (lm *LockManager) TryAcquireLock(transactionID uint32, key string, lockType LockType) error {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	if !lm.canGrantLock(key, lockType, transactionID) {
		return fmt.Errorf("lock not available for transaction %d on key %s", transactionID, key)
	}

	lock := &Lock{
		TransactionID: transactionID,
		Key:           key,
		Type:          lockType,
		AcquiredAt:    time.Now(),
	}

	lm.grantLock(lock)
	return nil
}

// ReleaseLock releases a specific lock
func (lm *LockManager) ReleaseLock(transactionID uint32, key string, lockType LockType) error {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	return lm.releaseLock(transactionID, key, lockType)
}

// ReleaseAllLocks releases all locks held by a transaction
func (lm *LockManager) ReleaseAllLocks(transactionID uint32) error {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	locks, exists := lm.transactionLocks[transactionID]
	if !exists {
		return nil // No locks to release
	}

	// Release all locks held by this transaction
	for _, lock := range locks {
		if err := lm.releaseLock(transactionID, lock.Key, lock.Type); err != nil {
			return fmt.Errorf("failed to release lock %s for transaction %d: %w", lock.Key, transactionID, err)
		}
	}

	return nil
}

// HasLock checks if a transaction has a specific lock
func (lm *LockManager) HasLock(transactionID uint32, key string, lockType LockType) bool {
	lm.mutex.RLock()
	defer lm.mutex.RUnlock()

	locks, exists := lm.transactionLocks[transactionID]
	if !exists {
		return false
	}

	for _, lock := range locks {
		if lock.Key == key && lock.Type == lockType {
			return true
		}
	}
	return false
}

// canGrantLock checks if a lock can be granted immediately
func (lm *LockManager) canGrantLock(key string, lockType LockType, transactionID uint32) bool {
	existingLocks := lm.lockTable[key]

	// If no existing locks, grant immediately
	if len(existingLocks) == 0 {
		return true
	}

	for _, existingLock := range existingLocks {
		// If same transaction already has the lock, allow (upgrade/downgrade)
		if existingLock.TransactionID == transactionID {
			continue
		}

		// Check compatibility
		if !lm.areLocksCompatible(existingLock.Type, lockType) {
			return false
		}
	}

	return true
}

// areLocksCompatible checks if two lock types are compatible
func (lm *LockManager) areLocksCompatible(existing, requested LockType) bool {
	// Read locks are compatible with other read locks
	if existing == ReadLock && requested == ReadLock {
		return true
	}

	// Write locks are not compatible with any other locks
	return false
}

// grantLock grants a lock to a transaction
func (lm *LockManager) grantLock(lock *Lock) {
	// Add to lock table
	lm.lockTable[lock.Key] = append(lm.lockTable[lock.Key], lock)

	// Add to transaction locks
	lm.transactionLocks[lock.TransactionID] = append(lm.transactionLocks[lock.TransactionID], lock)
}

// releaseLock releases a specific lock
func (lm *LockManager) releaseLock(transactionID uint32, key string, lockType LockType) error {
	// Remove from lock table
	locks := lm.lockTable[key]
	newLocks := make([]*Lock, 0, len(locks))
	found := false

	for _, lock := range locks {
		if lock.TransactionID == transactionID && lock.Type == lockType {
			found = true
			continue
		}
		newLocks = append(newLocks, lock)
	}

	if !found {
		return fmt.Errorf("lock not found for transaction %d on key %s", transactionID, key)
	}

	if len(newLocks) == 0 {
		delete(lm.lockTable, key)
	} else {
		lm.lockTable[key] = newLocks
	}

	// Remove from transaction locks
	txnLocks := lm.transactionLocks[transactionID]
	newTxnLocks := make([]*Lock, 0, len(txnLocks))

	for _, lock := range txnLocks {
		if lock.Key == key && lock.Type == lockType {
			continue
		}
		newTxnLocks = append(newTxnLocks, lock)
	}

	if len(newTxnLocks) == 0 {
		delete(lm.transactionLocks, transactionID)
	} else {
		lm.transactionLocks[transactionID] = newTxnLocks
	}

	// Check if any waiting requests can now be granted
	lm.processWaitingRequests(key)

	return nil
}

// processWaitingRequests processes waiting lock requests for a key
func (lm *LockManager) processWaitingRequests(key string) {
	waitingQueue := lm.waitingRequests[key]
	if len(waitingQueue) == 0 {
		return
	}

	newQueue := make([]*LockRequest, 0)

	for _, request := range waitingQueue {
		if lm.canGrantLock(key, request.Type, request.TransactionID) {
			// Grant the lock
			lock := &Lock{
				TransactionID: request.TransactionID,
				Key:           key,
				Type:          request.Type,
				AcquiredAt:    time.Now(),
			}

			lm.grantLock(lock)

			// Notify the waiting goroutine
			select {
			case request.AcquiredCh <- nil:
			default:
				// Channel might be closed due to timeout
			}
		} else {
			newQueue = append(newQueue, request)
		}
	}

	if len(newQueue) == 0 {
		delete(lm.waitingRequests, key)
	} else {
		lm.waitingRequests[key] = newQueue
	}
}

// removeWaitingRequest removes a waiting request (used for timeout)
func (lm *LockManager) removeWaitingRequest(transactionID uint32, key string) {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	waitingQueue := lm.waitingRequests[key]
	newQueue := make([]*LockRequest, 0)

	for _, request := range waitingQueue {
		if request.TransactionID != transactionID {
			newQueue = append(newQueue, request)
		}
	}

	if len(newQueue) == 0 {
		delete(lm.waitingRequests, key)
	} else {
		lm.waitingRequests[key] = newQueue
	}
}

// wouldCauseDeadlock performs simple deadlock detection
func (lm *LockManager) wouldCauseDeadlock(transactionID uint32, key string) bool {
	// Simple deadlock detection: check if any transaction holding locks on this key
	// is waiting for locks held by this transaction

	locksOnKey := lm.lockTable[key]
	myLocks := lm.transactionLocks[transactionID]

	for _, lockOnKey := range locksOnKey {
		otherTxnID := lockOnKey.TransactionID
		if otherTxnID == transactionID {
			continue
		}

		// Check if the other transaction is waiting for any key that I hold locks on
		for _, myLock := range myLocks {
			waitingOnMyKey := lm.waitingRequests[myLock.Key]
			for _, waitingRequest := range waitingOnMyKey {
				if waitingRequest.TransactionID == otherTxnID {
					return true // Deadlock detected
				}
			}
		}
	}

	return false
}

// GetLockStatistics returns lock statistics for monitoring
func (lm *LockManager) GetLockStatistics() map[string]any {
	lm.mutex.RLock()
	defer lm.mutex.RUnlock()

	stats := map[string]any{
		"total_keys_locked":   len(lm.lockTable),
		"active_transactions": len(lm.transactionLocks),
		"waiting_requests":    0,
	}

	totalWaiting := 0
	for _, requests := range lm.waitingRequests {
		totalWaiting += len(requests)
	}
	stats["waiting_requests"] = totalWaiting

	return stats
}
