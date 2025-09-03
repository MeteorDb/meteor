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
	RangeLock      // for locking key ranges (e.g., keys from "user_000" to "user_999")
	PredicateLock  // for locking based on query conditions (e.g., WHERE age > 25)
)

func (lt LockType) String() string {
	switch lt {
	case ReadLock:
		return "READ"
	case WriteLock:
		return "WRITE"
	case RangeLock:
		return "RANGE"
	case PredicateLock:
		return "PREDICATE"
	default:
		return "UNKNOWN"
	}
}

// Lock represents a single lock held by a transaction
type Lock struct {
	TransactionID uint32
	Key           string  // For point locks, the specific key. For range/gap/predicate locks, this might be empty or used as an identifier
	Type          LockType
	AcquiredAt    time.Time
	
	// Fields for range locks
	StartKey    string // for range locks, the starting key of the range
	EndKey      string // for range locks, the ending key of the range
	Predicate   string // for predicate locks, the condition being locked
}

// LockRequest represents a request for acquiring a lock
type LockRequest struct {
	TransactionID uint32
	Key           string
	Type          LockType
	AcquiredCh    chan error
	
	// Fields for range and predicate lock requests
	StartKey    string
	EndKey      string
	Predicate   string
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

// AcquireRangeLock acquires a range lock for [startKey, endKey]
func (lm *LockManager) AcquireRangeLock(transactionID uint32, startKey, endKey string, timeout time.Duration) error {
	lm.mutex.Lock()
	
	// Check if range lock can be granted immediately
	if lm.canGrantRangeLock(startKey, endKey, transactionID) {
		lock := &Lock{
			TransactionID: transactionID,
			Key:           fmt.Sprintf("range:%s:%s", startKey, endKey),
			Type:          RangeLock,
			AcquiredAt:    time.Now(),
			StartKey:      startKey,
			EndKey:        endKey,
		}
		
		lm.grantRangeLock(lock)
		lm.mutex.Unlock()
		return nil
	}
	
	// Check for deadlock before adding to waiting queue
	if lm.wouldCauseDeadlock(transactionID, fmt.Sprintf("range:%s:%s", startKey, endKey)) {
		lm.mutex.Unlock()
		return errors.New("deadlock detected")
	}
	
	// Add to waiting queue
	request := &LockRequest{
		TransactionID: transactionID,
		Key:           fmt.Sprintf("range:%s:%s", startKey, endKey),
		Type:          RangeLock,
		StartKey:      startKey,
		EndKey:        endKey,
		AcquiredCh:    make(chan error, 1),
	}
	
	lm.waitingRequests[request.Key] = append(lm.waitingRequests[request.Key], request)
	lm.mutex.Unlock()
	
	// Wait for lock to be granted or timeout
	select {
	case err := <-request.AcquiredCh:
		return err
	case <-time.After(timeout):
		lm.removeWaitingRequest(transactionID, request.Key)
		return fmt.Errorf("range lock acquisition timeout for transaction %d on range [%s, %s]", transactionID, startKey, endKey)
	}
}

// AcquirePredicateLock acquires a predicate lock for a condition
func (lm *LockManager) AcquirePredicateLock(transactionID uint32, predicate string, timeout time.Duration) error {
	lm.mutex.Lock()
	
	// Check if predicate lock can be granted immediately
	if lm.canGrantPredicateLock(predicate, transactionID) {
		lock := &Lock{
			TransactionID: transactionID,
			Key:           fmt.Sprintf("predicate:%s", predicate),
			Type:          PredicateLock,
			AcquiredAt:    time.Now(),
			Predicate:     predicate,
		}
		
		lm.grantPredicateLock(lock)
		lm.mutex.Unlock()
		return nil
	}
	
	// Check for deadlock
	if lm.wouldCauseDeadlock(transactionID, fmt.Sprintf("predicate:%s", predicate)) {
		lm.mutex.Unlock()
		return errors.New("deadlock detected")
	}
	
	// Add to waiting queue
	request := &LockRequest{
		TransactionID: transactionID,
		Key:           fmt.Sprintf("predicate:%s", predicate),
		Type:          PredicateLock,
		Predicate:     predicate,
		AcquiredCh:    make(chan error, 1),
	}
	
	lm.waitingRequests[request.Key] = append(lm.waitingRequests[request.Key], request)
	lm.mutex.Unlock()
	
	// Wait for lock to be granted or timeout
	select {
	case err := <-request.AcquiredCh:
		return err
	case <-time.After(timeout):
		lm.removeWaitingRequest(transactionID, request.Key)
		return fmt.Errorf("predicate lock acquisition timeout for transaction %d on predicate %s", transactionID, predicate)
	}
}

// Helper methods for range lock conflict detection
func (lm *LockManager) canGrantRangeLock(startKey, endKey string, transactionID uint32) bool {
	// Check for conflicts with existing point locks in the range
	for key, locks := range lm.lockTable {
		if key >= startKey && key <= endKey {
			for _, lock := range locks {
				if lock.TransactionID != transactionID {
					return false
				}
			}
		}
	}
	
	// Check for conflicts with existing range locks
	// TODO: Not implemented correctly, need to check for overlapping ranges
	rangeLockKey := fmt.Sprintf("range:%s:%s", startKey, endKey)
	if locks, exists := lm.lockTable[rangeLockKey]; exists {
		for _, lock := range locks {
			if lock.TransactionID != transactionID {
				// Check for overlapping ranges
				if lm.rangesOverlap(startKey, endKey, lock.StartKey, lock.EndKey) {
					return false
				}
			}
		}
	}
	
	return true
}

func (lm *LockManager) canGrantPredicateLock(predicate string, transactionID uint32) bool {
	// Predicate locks can conflict with overlapping predicates
	predicateLockKey := fmt.Sprintf("predicate:%s", predicate)
	if locks, exists := lm.lockTable[predicateLockKey]; exists {
		for _, lock := range locks {
			if lock.TransactionID != transactionID {
				// For now, assume any predicate conflicts with the same predicate
				// In a more sophisticated implementation, we'd analyze predicate overlap
				return false
			}
		}
	}
	return true
}

func (lm *LockManager) grantRangeLock(lock *Lock) {
	lm.lockTable[lock.Key] = append(lm.lockTable[lock.Key], lock)
	lm.transactionLocks[lock.TransactionID] = append(lm.transactionLocks[lock.TransactionID], lock)
}

func (lm *LockManager) grantPredicateLock(lock *Lock) {
	lm.lockTable[lock.Key] = append(lm.lockTable[lock.Key], lock)
	lm.transactionLocks[lock.TransactionID] = append(lm.transactionLocks[lock.TransactionID], lock)
}

func (lm *LockManager) rangesOverlap(start1, end1, start2, end2 string) bool {
	return !(end1 < start2 || end2 < start1)
}
