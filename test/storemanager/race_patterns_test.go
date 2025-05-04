package storemanager

import (
	"fmt"
	"math/rand"
	"meteor/internal/common"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestReadWriteRace tests the classic readers-writers problem
// where multiple readers can access the data structure concurrently,
// but writers need exclusive access.
func TestReadWriteRace(t *testing.T) {
	// Get the shared StoreManager and reset it
	sm := GetSharedStoreManager(t)
	ResetSharedStoreManager(t)

	// Define test parameters
	numReaders := 20
	numWriters := 5
	numOperationsPerThread := 1000
	keyRange := 10

	// Initialize keys
	for i := 0; i < keyRange; i++ {
		key := fmt.Sprintf("key-%d", i)
		cmd := &common.Command{
			Operation: "PUT",
			Args:      []string{key, fmt.Sprintf("initial-value-%d", i)},
		}
		_, err := sm.Put(cmd)
		if err != nil {
			t.Fatalf("Failed to initialize key %s: %v", key, err)
		}
	}

	// Create wait groups for readers and writers
	var readersWg sync.WaitGroup
	var writersWg sync.WaitGroup
	readersWg.Add(numReaders)
	writersWg.Add(numWriters)

	// Track operations
	var readCount atomic.Int64
	var writeCount atomic.Int64
	var errorCount atomic.Int64

	// Start readers
	for i := 0; i < numReaders; i++ {
		go func(readerID int) {
			defer readersWg.Done()

			// Each reader performs get operations
			for j := 0; j < numOperationsPerThread; j++ {
				keyIdx := rand.Intn(keyRange)
				key := fmt.Sprintf("key-%d", keyIdx)

				getCmd := &common.Command{
					Operation: "GET",
					Args:      []string{key},
				}
				_, err := sm.Get(getCmd)
				if err != nil {
					t.Errorf("Reader %d: GET operation failed for key %s: %v", readerID, key, err)
					errorCount.Add(1)
				} else {
					readCount.Add(1)
				}
			}
		}(i)
	}

	// Start writers
	for i := 0; i < numWriters; i++ {
		go func(writerID int) {
			defer writersWg.Done()

			// Each writer performs put operations
			for j := 0; j < numOperationsPerThread; j++ {
				keyIdx := rand.Intn(keyRange)
				key := fmt.Sprintf("key-%d", keyIdx)
				value := fmt.Sprintf("value-w%d-%d", writerID, j)

				putCmd := &common.Command{
					Operation: "PUT",
					Args:      []string{key, value},
				}
				_, err := sm.Put(putCmd)
				if err != nil {
					t.Errorf("Writer %d: PUT operation failed for key %s: %v", writerID, key, err)
					errorCount.Add(1)
				} else {
					writeCount.Add(1)
				}
			}
		}(i)
	}

	// Wait for all operations to complete
	readersWg.Wait()
	writersWg.Wait()

	// Report stats
	t.Logf("Read-Write Race Test: %d reads, %d writes, %d errors", 
		readCount.Load(), writeCount.Load(), errorCount.Load())
}

// TestProducerConsumerPattern tests a scenario where some threads "produce"
// data by inserting keys, and other threads "consume" data by reading and 
// deleting those keys.
func TestProducerConsumerPattern(t *testing.T) {
	// Get the shared StoreManager and reset it
	sm := GetSharedStoreManager(t)
	ResetSharedStoreManager(t)

	// Define test parameters
	numProducers := 8
	numConsumers := 8
	keysPerProducer := 100
	timeoutSeconds := 10

	// A channel used to signal when to stop the test
	done := make(chan struct{})
	
	// After the timeout, signal completion
	time.AfterFunc(time.Duration(timeoutSeconds)*time.Second, func() {
		close(done)
	})

	// Create a wait group for producers and consumers
	var wg sync.WaitGroup
	wg.Add(numProducers + numConsumers)

	// Create a queue to track keys that have been produced
	queue := make(chan string, numProducers*keysPerProducer)
	
	// Counters
	var producedCount atomic.Int64
	var consumedCount atomic.Int64
	var deleteErrors atomic.Int64
	var getErrors atomic.Int64

	// Start producers
	for i := 0; i < numProducers; i++ {
		go func(producerID int) {
			defer wg.Done()

			// Each producer creates keysPerProducer keys
			for j := 0; j < keysPerProducer; j++ {
				select {
				case <-done:
					return
				default:
					// Continue producing
				}

				key := fmt.Sprintf("producer-%d-key-%d", producerID, j)
				value := fmt.Sprintf("value-%d-%d", producerID, j)

				putCmd := &common.Command{
					Operation: "PUT",
					Args:      []string{key, value},
				}
				_, err := sm.Put(putCmd)
				if err != nil {
					t.Errorf("Producer %d: PUT operation failed for key %s: %v", producerID, key, err)
					continue
				}

				// Add the key to the queue for consumers
				queue <- key
				producedCount.Add(1)
			}
		}(i)
	}

	// Start consumers
	for i := 0; i < numConsumers; i++ {
		go func(consumerID int) {
			defer wg.Done()

			for {
				select {
				case <-done:
					return
				case key, ok := <-queue:
					if !ok {
						return
					}

					// First get the key to verify it exists
					getCmd := &common.Command{
						Operation: "GET",
						Args:      []string{key},
					}
					value, err := sm.Get(getCmd)
					if err != nil {
						t.Errorf("Consumer %d: GET operation failed for key %s: %v", consumerID, key, err)
						getErrors.Add(1)
						continue
					}

					if value == nil || string(value) == "-1" || string(value) == "-2" {
						t.Errorf("Consumer %d: Key %s not found or already deleted", consumerID, key)
						continue
					}

					// Then delete the key
					deleteCmd := &common.Command{
						Operation: "DELETE",
						Args:      []string{key},
					}
					_, err = sm.Delete(deleteCmd)
					if err != nil {
						t.Errorf("Consumer %d: DELETE operation failed for key %s: %v", consumerID, key, err)
						deleteErrors.Add(1)
						continue
					}

					consumedCount.Add(1)
				}
			}
		}(i)
	}

	// Wait for all producers to finish
	time.Sleep(time.Duration(timeoutSeconds/2) * time.Second)
	
	// Close the queue when all producers are done or timeout
	close(queue)
	
	// Wait for all goroutines to complete
	wg.Wait()

	// Report stats
	t.Logf("Producer-Consumer Test: %d keys produced, %d keys consumed", 
		producedCount.Load(), consumedCount.Load())
	t.Logf("Errors: %d get errors, %d delete errors", 
		getErrors.Load(), deleteErrors.Load())

	// Check if all keys were properly consumed
	remainingKeys := producedCount.Load() - consumedCount.Load()
	if remainingKeys > 0 {
		t.Logf("Warning: %d keys were not consumed", remainingKeys)
	}
}

// TestHighFrequencyReadsWrites tests a scenario with extremely high frequency
// of reads and writes to detect any race conditions or deadlocks.
func TestHighFrequencyReadsWrites(t *testing.T) {
	// Get the shared StoreManager and reset it
	sm := GetSharedStoreManager(t)
	ResetSharedStoreManager(t)

	// Define test parameters
	numThreads := 20
	testDurationSeconds := 5
	keyRange := 20

	// Initialize keys with initial values
	for i := 0; i < keyRange; i++ {
		key := fmt.Sprintf("key-%d", i)
		cmd := &common.Command{
			Operation: "PUT",
			Args:      []string{key, "0"},
		}
		_, err := sm.Put(cmd)
		if err != nil {
			t.Fatalf("Failed to initialize key %s: %v", key, err)
		}
	}

	// Create a channel to signal when to stop
	done := make(chan struct{})
	
	// Set a timer to stop the test after the specified duration
	time.AfterFunc(time.Duration(testDurationSeconds)*time.Second, func() {
		close(done)
	})

	// Create a wait group for all threads
	var wg sync.WaitGroup
	wg.Add(numThreads)

	// Operation counters
	var readCount atomic.Int64
	var writeCount atomic.Int64
	var deleteCount atomic.Int64
	var errorCount atomic.Int64

	// Start all threads
	for i := 0; i < numThreads; i++ {
		go func(threadID int) {
			defer wg.Done()
			
			// Local random number generator
			rnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(threadID)))

			// Keep performing operations until signaled to stop
			for {
				select {
				case <-done:
					return
				default:
					// Continue with operations
				}

				// Choose a random key
				keyIdx := rnd.Intn(keyRange)
				key := fmt.Sprintf("key-%d", keyIdx)

				// Perform a random operation (50% GET, 40% PUT, 10% DELETE)
				opRand := rnd.Intn(100)
				if opRand < 50 {
					// GET operation
					getCmd := &common.Command{
						Operation: "GET",
						Args:      []string{key},
					}
					_, err := sm.Get(getCmd)
					if err != nil {
						errorCount.Add(1)
					} else {
						readCount.Add(1)
					}
				} else if opRand < 90 {
					// PUT operation
					value := fmt.Sprintf("value-%d-%d", threadID, rnd.Intn(1000))
					putCmd := &common.Command{
						Operation: "PUT",
						Args:      []string{key, value},
					}
					_, err := sm.Put(putCmd)
					if err != nil {
						errorCount.Add(1)
					} else {
						writeCount.Add(1)
					}
				} else {
					// DELETE operation
					deleteCmd := &common.Command{
						Operation: "DELETE",
						Args:      []string{key},
					}
					_, err := sm.Delete(deleteCmd)
					if err != nil {
						errorCount.Add(1)
					} else {
						deleteCount.Add(1)
					}
				}
			}
		}(i)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Report stats
	totalOps := readCount.Load() + writeCount.Load() + deleteCount.Load()
	opsPerSecond := totalOps / int64(testDurationSeconds)
	
	t.Logf("High Frequency Test: %d total operations in %d seconds (%d ops/sec)",
		totalOps, testDurationSeconds, opsPerSecond)
	t.Logf("Operations: %d reads, %d writes, %d deletes, %d errors",
		readCount.Load(), writeCount.Load(), deleteCount.Load(), errorCount.Load())
	
	// Verify final state of keys to ensure they still exist
	missingKeys := 0
	for i := 0; i < keyRange; i++ {
		key := fmt.Sprintf("key-%d", i)
		cmd := &common.Command{
			Operation: "GET",
			Args:      []string{key},
		}
		val, err := sm.Get(cmd)
		if err != nil {
			t.Errorf("Verification failed for key %s: %v", key, err)
		} else if string(val) == "-1" || string(val) == "-2" {
			missingKeys++
		}
	}
	
	t.Logf("%d out of %d keys were deleted during the test", missingKeys, keyRange)
} 