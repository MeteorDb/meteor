package transactionmanager

import (
	"errors"
	"meteor/internal/common"
	"meteor/internal/lockmanager"
	"meteor/internal/store"
	"meteor/internal/walmanager"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

type TransactionManager struct {
	transactionStoreMap map[uint32]store.Store
	connToTransactionIdsMap map[*net.Conn][]uint32
	txnToIsolationLevelMap map[uint32]string
	// GSN at transaction start for snapshot isolation
	txnStartGsnMap map[uint32]uint32
	walManager *walmanager.WalManager
	lockManager *lockmanager.LockManager
	currentTransactionId atomic.Uint32
	transactionIdBatchStart uint32
	transactionIdBatchEnd uint32
	m sync.Mutex
}

func NewTransactionManager(walManager *walmanager.WalManager) (*TransactionManager, error) {
	transactionIdBatchStart, transactionIdBatchEnd := walManager.AllocateTransactionIdBatch()
	transactionManager := &TransactionManager{
		walManager: walManager,
		transactionStoreMap: make(map[uint32]store.Store),
		connToTransactionIdsMap: make(map[*net.Conn][]uint32),
		txnToIsolationLevelMap: make(map[uint32]string),
		txnStartGsnMap: make(map[uint32]uint32),
		lockManager: lockmanager.NewLockManager(),
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
	
	transactionStore, ok := tm.transactionStoreMap[transactionRow.TransactionId]
	if !ok {
		transactionStore = store.NewBufferStore()
		tm.transactionStoreMap[transactionRow.TransactionId] = transactionStore
	}
	transactionStore.Put(transactionRow.Payload.Key, transactionRow.Payload.NewValue)

	return nil
}

func (tm *TransactionManager) GetTransactionStore(transactionId uint32) store.Store {
	store, ok := tm.transactionStoreMap[transactionId]
	if !ok {
		return nil
	}
	return store
}

// This method is used to finish a transaction. Applicable for both commits and rollbacks.
// It is used to release all locks for the transaction and clean up the transaction states.
func (tm *TransactionManager) ClearTransactionStore(transactionId uint32) {
	// Release all locks for this transaction
	_ = tm.lockManager.ReleaseAllLocks(transactionId)
	
	// Clean up transaction state
	delete(tm.transactionStoreMap, transactionId)
	delete(tm.txnToIsolationLevelMap, transactionId)
	delete(tm.txnStartGsnMap, transactionId)
}

func (tm *TransactionManager) isTransactionIdAllowedForConnection(transactionId uint32, conn *net.Conn) bool {
	isNewTransactionId := tm.IsNewTransactionId(transactionId)

	if isNewTransactionId {
		return true
	}

	transactionIds, ok := tm.connToTransactionIdsMap[conn]
	if !ok {
		return false
	}
	return slices.Contains(transactionIds, transactionId)
}

func (tm *TransactionManager) IsNewTransactionId(transactionId uint32) bool {
	for tId := range tm.transactionStoreMap {
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
		transactionIds = tm.connToTransactionIdsMap[conn]
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

func (tm *TransactionManager) GetStoreByTransactionId(transactionId uint32, conn *net.Conn) (store.Store, error) {
	if !tm.isTransactionIdAllowedForConnection(transactionId, conn) {
		return nil,errors.New("transaction id not allowed for connection")
	}

	store, ok := tm.transactionStoreMap[transactionId]
	if !ok {
		return nil, nil
	}
	return store, nil
}

func (tm *TransactionManager) EnsureIsolationLevel(transactionId uint32, isolationLevel string) error {
	txnIsolationLevel, ok := tm.txnToIsolationLevelMap[transactionId]
	if !ok {
		tm.txnToIsolationLevelMap[transactionId] = isolationLevel
	} else if txnIsolationLevel != isolationLevel {
		return errors.New("transaction isolation level mismatch: " + txnIsolationLevel + " != " + isolationLevel)
	}
	return nil
}

func (tm *TransactionManager) GetIsolationLevel(transactionId uint32) (string, error) {
	txnIsolationLevel, ok := tm.txnToIsolationLevelMap[transactionId]
	if !ok {
		// if not found, default to read_COMMITTED
		tm.txnToIsolationLevelMap[transactionId] = common.TXN_ISOLATION_READ_COMMITTED
		return common.TXN_ISOLATION_READ_COMMITTED, nil
	}
	return txnIsolationLevel, nil
}

// SetTransactionStartGsn sets the GSN at transaction start for snapshot isolation
func (tm *TransactionManager) SetTransactionStartGsn(transactionId uint32, gsn uint32) {
	tm.txnStartGsnMap[transactionId] = gsn
}

// GetTransactionStartGsn gets the GSN at transaction start for snapshot isolation
func (tm *TransactionManager) GetTransactionStartGsn(transactionId uint32) (uint32, bool) {
	gsn, exists := tm.txnStartGsnMap[transactionId]
	return gsn, exists
}

// AcquireReadLock acquires appropriate read locks based on isolation level
func (tm *TransactionManager) AcquireReadLock(transactionId uint32, key string, isolationLevel string) error {
	timeout := 30 * time.Second
	
	switch isolationLevel {
	case common.TXN_ISOLATION_SNAPSHOT_ISOLATION:
		// No locks needed for reads in snapshot isolation
		return nil
		
	case common.TXN_ISOLATION_READ_COMMITTED,
		 common.TXN_ISOLATION_REPEATABLE_READ,
		 common.TXN_ISOLATION_SERIALIZABLE:
		return tm.lockManager.AcquireLock(transactionId, key, lockmanager.ReadLock, timeout)
		
	default:
		return errors.New("unknown isolation level: " + isolationLevel)
	}
}

// AcquireWriteLock acquires appropriate write locks based on isolation level
func (tm *TransactionManager) AcquireWriteLock(transactionId uint32, key string, isolationLevel string) error {
	timeout := 30 * time.Second
	
	switch isolationLevel {
	case common.TXN_ISOLATION_READ_COMMITTED,
		 common.TXN_ISOLATION_REPEATABLE_READ,
		 common.TXN_ISOLATION_SNAPSHOT_ISOLATION,
		 common.TXN_ISOLATION_SERIALIZABLE:
		// All isolation levels need write locks
		return tm.lockManager.AcquireLock(transactionId, key, lockmanager.WriteLock, timeout)
		
	default:
		return errors.New("unknown isolation level: " + isolationLevel)
	}
}

// ReleaseReadLock releases read locks based on isolation level
func (tm *TransactionManager) ReleaseReadLock(transactionId uint32, key string, isolationLevel string) error {
	switch isolationLevel {
	case common.TXN_ISOLATION_READ_COMMITTED:
		// Release immediately for read committed
		return tm.lockManager.ReleaseLock(transactionId, key, lockmanager.ReadLock)
		
	case common.TXN_ISOLATION_REPEATABLE_READ,
		 common.TXN_ISOLATION_SERIALIZABLE:
		// Locks are held until transaction end, don't release here
		return nil
		
	case common.TXN_ISOLATION_SNAPSHOT_ISOLATION:
		// No read locks in snapshot isolation
		return nil
		
	default:
		return errors.New("unknown isolation level: " + isolationLevel)
	}
}

// ReadValue reads a value considering isolation level and transaction state
func (tm *TransactionManager) ReadValue(transactionId uint32, key string, bufferStore store.Store, conn *net.Conn) (*common.V, error) {
	isolationLevel, err := tm.GetIsolationLevel(transactionId)
	if err != nil {
		return nil, err
	}

	// ALWAYS check transaction store first - this fixes the read order issue
	transactionStore, err := tm.GetStoreByTransactionId(transactionId, conn)
	if err != nil {
		return nil, err
	}

	if transactionStore != nil {
		value := transactionStore.Get(key)
		if value != nil {
			return value, nil
		}
	}

	// Read from buffer store based on isolation level
	switch isolationLevel {
	case common.TXN_ISOLATION_READ_COMMITTED:
		// Read the latest committed value
		return bufferStore.Get(key), nil

	case common.TXN_ISOLATION_REPEATABLE_READ:
		// Read committed value (same as read committed for buffer store reads)
		return bufferStore.Get(key), nil

	case common.TXN_ISOLATION_SNAPSHOT_ISOLATION:
		// Read data that was committed at transaction start
		startGsn, exists := tm.GetTransactionStartGsn(transactionId)
		if !exists {
			return nil, errors.New("transaction start GSN not found for snapshot isolation")
		}
		
		// Find the latest version that was committed before or at transaction start
		return tm.getVersionAtGsn(key, startGsn, bufferStore), nil

	case common.TXN_ISOLATION_SERIALIZABLE:
		return bufferStore.Get(key), nil

	default:
		return nil, errors.New("unknown isolation level: " + isolationLevel)
	}
}

// getVersionAtGsn gets the version of a key that existed at or before the given GSN
// This method implements snapshot isolation by finding the highest GSN version <= maxGsn
func (tm *TransactionManager) getVersionAtGsn(key string, maxGsn uint32, bufferStore store.Store) *common.V {
	// TODO: In the future, this method will need to search across multiple storage levels:
	// 1. Current buffer store (in-memory) ✅ IMPLEMENTED
	// 2. Immutable stores (in-memory, being flushed) 
	// 3. Disk-based LSM tree levels (SSTables on disk)
	// For now, we only search the buffer store
	
	// Use the Store interface method for version traversal - this works for all store types
	return bufferStore.GetVersionAtOrBeforeGsn(key, maxGsn)
}

// ValidateWrite validates if a write is allowed based on isolation level and conflict detection
func (tm *TransactionManager) ValidateWrite(transactionId uint32, key string, bufferStore store.Store, conn *net.Conn) error {
	isolationLevel, err := tm.GetIsolationLevel(transactionId)
	if err != nil {
		return err
	}
	
	switch isolationLevel {
	case common.TXN_ISOLATION_READ_COMMITTED,
		 common.TXN_ISOLATION_REPEATABLE_READ,
		 common.TXN_ISOLATION_SERIALIZABLE:
		// Basic write validation - write lock should be sufficient
		return nil
		
	case common.TXN_ISOLATION_SNAPSHOT_ISOLATION:
		// First committer wins - check if key was modified since transaction start
		return tm.validateFirstCommitterWins(transactionId, key, bufferStore)
		
	default:
		return errors.New("unknown isolation level: " + isolationLevel)
	}
}

// validateFirstCommitterWins implements first-committer-wins for snapshot isolation
func (tm *TransactionManager) validateFirstCommitterWins(transactionId uint32, key string, bufferStore store.Store) error {
	startGsn, exists := tm.GetTransactionStartGsn(transactionId)
	if !exists {
		return errors.New("transaction start GSN not found for snapshot isolation")
	}
	
	// Get the latest GSN for the key from buffer store
	latestGsn, err := bufferStore.GetLatestGsn(key)
	if err != nil {
		// Key doesn't exist in buffer store, so no conflict
		return nil
	}
	
	// If buffer store has a newer version than what existed at transaction start, there's a conflict
	if latestGsn > startGsn {
		return errors.New("write-write conflict detected - another transaction committed first")
	}
	
	return nil
}

// AcquireRangeLock acquires a range lock for serializable isolation
func (tm *TransactionManager) AcquireRangeLock(transactionId uint32, startKey, endKey string) error {
	isolationLevel, err := tm.GetIsolationLevel(transactionId)
	if err != nil {
		return err
	}

	// Only acquire range lock for serializable isolation
	if isolationLevel != common.TXN_ISOLATION_SERIALIZABLE {
		return nil
	}

	timeout := 30 * time.Second
	return tm.lockManager.AcquireRangeLock(transactionId, startKey, endKey, timeout)
}

// AcquirePredicateLock acquires a predicate lock for serializable isolation
func (tm *TransactionManager) AcquirePredicateLock(transactionId uint32, predicate string) error {
	isolationLevel, err := tm.GetIsolationLevel(transactionId)
	if err != nil {
		return err
	}

	// Only acquire predicate lock for serializable isolation
	if isolationLevel != common.TXN_ISOLATION_SERIALIZABLE {
		return nil
	}

	timeout := 30 * time.Second
	return tm.lockManager.AcquirePredicateLock(transactionId, predicate, timeout)
}

// ReadRangeValues reads all key-value pairs in a range, respecting transaction isolation
func (tm *TransactionManager) ReadRangeValues(transactionId uint32, startKey, endKey string, bufferStore store.Store, conn *net.Conn) (map[string]*common.V, error) {
	isolationLevel, err := tm.GetIsolationLevel(transactionId)
	if err != nil {
		return nil, err
	}

	result := make(map[string]*common.V)

	// First, check transaction store for any modified keys in the range
	transactionStore, err := tm.GetStoreByTransactionId(transactionId, conn)
	if err != nil {
		return nil, err
	}

	if transactionStore != nil {
		result = transactionStore.ScanRange(startKey, endKey)
	}

	// Read from buffer store based on isolation level
	var bufferResults map[string]*common.V
	switch isolationLevel {
	case common.TXN_ISOLATION_READ_COMMITTED,
		 common.TXN_ISOLATION_REPEATABLE_READ,
		 common.TXN_ISOLATION_SERIALIZABLE:
		bufferResults = bufferStore.ScanRange(startKey, endKey)

	case common.TXN_ISOLATION_SNAPSHOT_ISOLATION:
		// For snapshot isolation, filter by transaction start GSN
		startGsn, exists := tm.GetTransactionStartGsn(transactionId)
		if !exists {
			return nil, errors.New("transaction start GSN not found for snapshot isolation")
		}
		
		// Get all keys in range and check their versions
		allInRange := bufferStore.ScanRange(startKey, endKey)
		bufferResults = make(map[string]*common.V)
		for key := range allInRange {
			if value := tm.getVersionAtGsn(key, startGsn, bufferStore); value != nil {
				bufferResults[key] = value
			}
		}

	default:
		return nil, errors.New("unknown isolation level: " + isolationLevel)
	}

	// Merge buffer store results, but transaction store takes precedence
	for key, value := range bufferResults {
		if _, exists := result[key]; !exists {
			result[key] = value
		}
	}

	return result, nil
}

// ReadPrefixValues reads all key-value pairs with a given prefix, respecting transaction isolation
func (tm *TransactionManager) ReadPrefixValues(transactionId uint32, prefix string, bufferStore store.Store, conn *net.Conn) (map[string]*common.V, error) {
	isolationLevel, err := tm.GetIsolationLevel(transactionId)
	if err != nil {
		return nil, err
	}

	result := make(map[string]*common.V)

	// First, check transaction store for any modified keys with the prefix
	transactionStore, err := tm.GetStoreByTransactionId(transactionId, conn)
	if err != nil {
		return nil, err
	}

	if transactionStore != nil {
		result = transactionStore.ScanPrefix(prefix)
	}

	// Read from buffer store based on isolation level
	var bufferResults map[string]*common.V
	switch isolationLevel {
	case common.TXN_ISOLATION_READ_COMMITTED,
		 common.TXN_ISOLATION_REPEATABLE_READ,
		 common.TXN_ISOLATION_SERIALIZABLE:
		bufferResults = bufferStore.ScanPrefix(prefix)

	case common.TXN_ISOLATION_SNAPSHOT_ISOLATION:
		// For snapshot isolation, filter by transaction start GSN
		startGsn, exists := tm.GetTransactionStartGsn(transactionId)
		if !exists {
			return nil, errors.New("transaction start GSN not found for snapshot isolation")
		}
		
		// Get all keys with prefix and check their versions
		allWithPrefix := bufferStore.ScanPrefix(prefix)
		bufferResults = make(map[string]*common.V)
		for key := range allWithPrefix {
			if value := tm.getVersionAtGsn(key, startGsn, bufferStore); value != nil {
				bufferResults[key] = value
			}
		}

	default:
		return nil, errors.New("unknown isolation level: " + isolationLevel)
	}

	// Merge buffer store results, but transaction store takes precedence
	for key, value := range bufferResults {
		if _, exists := result[key]; !exists {
			result[key] = value
		}
	}

	return result, nil
}

// ReadFilteredValues reads all key-value pairs matching a filter function, respecting transaction isolation
func (tm *TransactionManager) ReadFilteredValues(transactionId uint32, filterFunc func(string, *common.V) bool, bufferStore store.Store, conn *net.Conn) (map[string]*common.V, error) {
	isolationLevel, err := tm.GetIsolationLevel(transactionId)
	if err != nil {
		return nil, err
	}

	result := make(map[string]*common.V)

	// First, check transaction store for any modified keys that match the filter
	transactionStore, err := tm.GetStoreByTransactionId(transactionId, conn)
	if err != nil {
		return nil, err
	}

	if transactionStore != nil {
		result = transactionStore.ScanWithFilter(filterFunc)
	}

	// Read from buffer store based on isolation level
	var bufferResults map[string]*common.V
	switch isolationLevel {
	case common.TXN_ISOLATION_READ_COMMITTED,
		 common.TXN_ISOLATION_REPEATABLE_READ,
		 common.TXN_ISOLATION_SERIALIZABLE:
		bufferResults = bufferStore.ScanWithFilter(filterFunc)

	case common.TXN_ISOLATION_SNAPSHOT_ISOLATION:
		// For snapshot isolation, filter by transaction start GSN
		startGsn, exists := tm.GetTransactionStartGsn(transactionId)
		if !exists {
			return nil, errors.New("transaction start GSN not found for snapshot isolation")
		}
		
		// Create a filter that also checks GSN
		snapshotFilter := func(key string, value *common.V) bool {
			// First check if the value existed at transaction start
			snapshotValue := tm.getVersionAtGsn(key, startGsn, bufferStore)
			if snapshotValue == nil {
				return false
			}
			// Apply the original filter to the snapshot value
			return filterFunc(key, snapshotValue)
		}
		
		bufferResults = bufferStore.ScanWithFilter(snapshotFilter)

	default:
		return nil, errors.New("unknown isolation level: " + isolationLevel)
	}

	// Merge buffer store results, but transaction store takes precedence
	for key, value := range bufferResults {
		if _, exists := result[key]; !exists {
			result[key] = value
		}
	}

	return result, nil
}
