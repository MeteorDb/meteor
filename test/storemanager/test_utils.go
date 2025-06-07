package storemanager

import (
	"fmt"
	"meteor/internal/common"
	"meteor/internal/storemanager"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Global shared StoreManager instance for all tests
var (
	// SharedSM is the shared StoreManager instance that all tests will use
	SharedSM *storemanager.StoreManager
	
	// setupOnce ensures setup is performed only once
	setupOnce sync.Once
	
	// setupError stores any error that occurred during setup
	setupError error
)

// GetSharedStoreManager returns the shared StoreManager instance
// If it doesn't exist yet, it creates one
func GetSharedStoreManager(t testing.TB) *storemanager.StoreManager {
	setupOnce.Do(func() {
		SharedSM, setupError = storemanager.NewStoreManager()
		if setupError != nil {
			t.Fatalf("Failed to create shared store manager: %v", setupError)
		}
	})
	
	if SharedSM == nil {
		t.Fatal("Failed to get shared store manager")
	}
	
	return SharedSM
}

// ResetSharedStoreManager resets the state of the shared StoreManager
func ResetSharedStoreManager(t testing.TB) {
	sm := GetSharedStoreManager(t)
	err := sm.Reset()
	if err != nil {
		t.Fatalf("Failed to reset shared store manager: %v", err)
	}
}

// ThreadSafeCounter is a thread-safe counter utility for tests
type ThreadSafeCounter struct {
	counter atomic.Int64
}

func NewThreadSafeCounter() *ThreadSafeCounter {
	return &ThreadSafeCounter{}
}

func (c *ThreadSafeCounter) Increment() int64 {
	return c.counter.Add(1)
}

func (c *ThreadSafeCounter) Get() int64 {
	return c.counter.Load()
}

// TestStats tracks various statistics for multi-threaded tests
type TestStats struct {
	Reads        *ThreadSafeCounter
	Writes       *ThreadSafeCounter
	Deletes      *ThreadSafeCounter
	Errors       *ThreadSafeCounter
	StartTime    time.Time
	ElapsedTime  time.Duration
	NumThreads   int
}

func NewTestStats(numThreads int) *TestStats {
	return &TestStats{
		Reads:      NewThreadSafeCounter(),
		Writes:     NewThreadSafeCounter(),
		Deletes:    NewThreadSafeCounter(),
		Errors:     NewThreadSafeCounter(),
		StartTime:  time.Now(),
		NumThreads: numThreads,
	}
}

func (s *TestStats) Stop() {
	s.ElapsedTime = time.Since(s.StartTime)
}

func (s *TestStats) TotalOperations() int64 {
	return s.Reads.Get() + s.Writes.Get() + s.Deletes.Get()
}

func (s *TestStats) OperationsPerSecond() int64 {
	if s.ElapsedTime == 0 {
		return 0
	}
	return int64(float64(s.TotalOperations()) / s.ElapsedTime.Seconds())
}

func (s *TestStats) LogResults(t *testing.T, testName string) {
	t.Logf("%s completed in %v", testName, s.ElapsedTime)
	t.Logf("Total operations: %d (%d ops/sec)", s.TotalOperations(), s.OperationsPerSecond())
	t.Logf("Operations by type: %d reads, %d writes, %d deletes", 
		s.Reads.Get(), s.Writes.Get(), s.Deletes.Get())
	t.Logf("Errors: %d", s.Errors.Get())
	t.Logf("Thread utilization: %.2f ops/thread", float64(s.TotalOperations())/float64(s.NumThreads))
}

// RunConcurrentOperations runs N threads performing random operations for a specified duration
func RunConcurrentOperations(
	t *testing.T,
	sm *storemanager.StoreManager,
	numThreads int,
	durationSeconds int,
	keyRange int,
	readPct, writePct, deletePct int,
	operationFunc func(threadID, iteration int, key string, stats *TestStats) error,
) *TestStats {
	// Create stats
	stats := NewTestStats(numThreads)
	
	// Create a done channel to signal when to stop
	done := make(chan struct{})
	
	// After the timeout, signal completion
	time.AfterFunc(time.Duration(durationSeconds)*time.Second, func() {
		close(done)
	})
	
	// Create a wait group for all goroutines
	var wg sync.WaitGroup
	wg.Add(numThreads)
	
	// Start all goroutines
	for i := 0; i < numThreads; i++ {
		go func(threadID int) {
			defer wg.Done()
			
			iteration := 0
			for {
				select {
				case <-done:
					return
				default:
					// Continue with operations
				}
				
				// Select a key to operate on
				keyIdx := (threadID*1000 + iteration) % keyRange
				key := fmt.Sprintf("key-%d", keyIdx)
				
				// Call the operation function
				if err := operationFunc(threadID, iteration, key, stats); err != nil {
					t.Errorf("Thread %d: Operation failed: %v", threadID, err)
					stats.Errors.Increment()
				}
				
				iteration++
			}
		}(i)
	}
	
	// Wait for all goroutines to complete
	wg.Wait()
	
	// Record elapsed time
	stats.Stop()
	
	return stats
}

// VerifyKeyValues verifies that all keys in the store have the expected values
func VerifyKeyValues(
	t *testing.T, 
	sm *storemanager.StoreManager, 
	expectedValues map[string]string,
) int {
	inconsistencies := 0
	for key, expectedValue := range expectedValues {
		cmd := &common.Command{
			Operation: "GET",
			Args:      []string{key},
		}
		actualValueBytes, err := sm.Get(cmd)
		if err != nil {
			t.Errorf("Verification GET failed for key %s: %v", key, err)
			inconsistencies++
			continue
		}
		
		actualValue := string(actualValueBytes)
		if actualValue != expectedValue {
			t.Errorf("Inconsistency for key %s: expected %q, got %q", 
				key, expectedValue, actualValue)
			inconsistencies++
		}
	}
	return inconsistencies
}

// InitializeKeys initializes a set of keys in the store with values
func InitializeKeys(
	t *testing.T,
	sm *storemanager.StoreManager,
	keyPrefix string,
	count int,
	valueFunc func(i int) string,
) error {
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("%s-%d", keyPrefix, i)
		value := valueFunc(i)
		
		cmd := &common.Command{
			Operation: "PUT",
			Args:      []string{key, value},
		}
		_, err := sm.Put(cmd)
		if err != nil {
			t.Errorf("Failed to initialize key %s: %v", key, err)
			return err
		}
	}
	return nil
} 