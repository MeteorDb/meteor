package storemanager

import (
	"fmt"
	"meteor/internal/common"
	"strconv"
	"sync"
	"testing"
	"time"
)

// TestSharedComprehensive runs a comprehensive suite of tests on the shared
// StoreManager to really stress test it in a multi-threaded environment
func TestSharedComprehensive(t *testing.T) {
	// Get the shared StoreManager and reset its state
	sm := GetSharedStoreManager(t)
	ResetSharedStoreManager(t)
	
	// Define test parameters
	numThreads := 10
	keyRange := 20
	operationsPerThread := 500
	
	// Initialize a set of counters that will be updated by different tests
	for i := 0; i < keyRange; i++ {
		key := fmt.Sprintf("counter-%d", i)
		cmd := &common.Command{
			Operation: "PUT",
			Args:      []string{key, "0"},
		}
		_, err := sm.Put(cmd)
		if err != nil {
			t.Fatalf("Failed to initialize counter %s: %v", key, err)
		}
	}
	
	// Track when each test stage completes
	var wg sync.WaitGroup
	
	// 1. Run a counter increment test (multiple threads increment the same counters)
	t.Run("IncrementCounters", func(t *testing.T) {
		wg.Add(numThreads)
		expectedIncrements := make([]int, keyRange)
		
		for i := 0; i < numThreads; i++ {
			go func(threadID int) {
				defer wg.Done()
				
				for j := 0; j < operationsPerThread; j++ {
					// Select counter in a deterministic pattern
					counterIdx := (threadID*operationsPerThread + j) % keyRange
					key := fmt.Sprintf("counter-%d", counterIdx)
					
					// Critical section: get-increment-put
					getCmd := &common.Command{
						Operation: "GET",
						Args:      []string{key},
					}
					valueBytes, err := sm.Get(getCmd)
					if err != nil {
						t.Errorf("Thread %d: GET failed for counter %s: %v", threadID, key, err)
						continue
					}
					
					currentValue, err := strconv.Atoi(string(valueBytes))
					if err != nil {
						t.Errorf("Thread %d: Failed to parse value %s: %v", threadID, string(valueBytes), err)
						continue
					}
					
					newValue := currentValue + 1
					putCmd := &common.Command{
						Operation: "PUT",
						Args:      []string{key, strconv.Itoa(newValue)},
					}
					_, err = sm.Put(putCmd)
					if err != nil {
						t.Errorf("Thread %d: PUT failed for counter %s: %v", threadID, key, err)
						continue
					}
					
					expectedIncrements[counterIdx]++
				}
			}(i)
		}
		
		// Wait for all threads to finish
		wg.Wait()
		
		// Verify the final counter values
		for i := 0; i < keyRange; i++ {
			key := fmt.Sprintf("counter-%d", i)
			getCmd := &common.Command{
				Operation: "GET",
				Args:      []string{key},
			}
			valueBytes, err := sm.Get(getCmd)
			if err != nil {
				t.Errorf("Verification GET failed for counter %s: %v", key, err)
				continue
			}
			
			finalValue, err := strconv.Atoi(string(valueBytes))
			if err != nil {
				t.Errorf("Failed to parse final value %s: %v", string(valueBytes), err)
				continue
			}
			
			expectedValue := expectedIncrements[i]
			if finalValue != expectedValue {
				t.Errorf("Race condition detected for counter %s: expected %d, got %d", 
					key, expectedValue, finalValue)
			}
		}
		
		// Report the first stage result
		size, _ := sm.Size()
		t.Logf("Completed increment counter test with %d threads, store size: %d", numThreads, size)
	})
	
	// 2. Run a concurrent PUT test that adds new unique keys
	t.Run("ConcurrentPut", func(t *testing.T) {
		wg.Add(numThreads)
		
		for i := 0; i < numThreads; i++ {
			go func(threadID int) {
				defer wg.Done()
				
				for j := 0; j < operationsPerThread; j++ {
					key := fmt.Sprintf("put-test-thread-%d-key-%d", threadID, j)
					value := fmt.Sprintf("value-%d-%d", threadID, j)
					
					cmd := &common.Command{
						Operation: "PUT",
						Args:      []string{key, value},
					}
					_, err := sm.Put(cmd)
					if err != nil {
						t.Errorf("Thread %d: PUT failed for key %s: %v", threadID, key, err)
					}
				}
			}(i)
		}
		
		// Wait for all threads to finish
		wg.Wait()
		
		// Verify a sample of the keys
		for i := 0; i < numThreads; i++ {
			// Check a few keys from each thread
			for j := 0; j < 10; j++ {
				key := fmt.Sprintf("put-test-thread-%d-key-%d", i, j)
				expectedValue := fmt.Sprintf("value-%d-%d", i, j)
				
				getCmd := &common.Command{
					Operation: "GET",
					Args:      []string{key},
				}
				valueBytes, err := sm.Get(getCmd)
				if err != nil {
					t.Errorf("Verification GET failed for key %s: %v", key, err)
					continue
				}
				
				actualValue := string(valueBytes)
				if actualValue != expectedValue {
					t.Errorf("Value mismatch for key %s: expected %q, got %q", 
						key, expectedValue, actualValue)
				}
			}
		}
		
		// Report the second stage result
		size, _ := sm.Size()
		t.Logf("Completed concurrent PUT test with %d threads, store size: %d", numThreads, size)
	})
	
	// 3. Run a concurrent mixed operations test (GET, PUT, DELETE)
	t.Run("MixedOperations", func(t *testing.T) {
		wg.Add(numThreads)
		
		// Define the key prefix for this test
		keyPrefix := "mixed-test-key"
		
		// Initialize a small set of keys for mixed operations
		for i := 0; i < keyRange; i++ {
			key := fmt.Sprintf("%s-%d", keyPrefix, i)
			cmd := &common.Command{
				Operation: "PUT",
				Args:      []string{key, fmt.Sprintf("initial-%d", i)},
			}
			_, err := sm.Put(cmd)
			if err != nil {
				t.Fatalf("Failed to initialize key %s: %v", key, err)
			}
		}
		
		// Start time for performance measurement
		startTime := time.Now()
		
		// Start threads to perform mixed operations
		for i := 0; i < numThreads; i++ {
			go func(threadID int) {
				defer wg.Done()
				
				for j := 0; j < operationsPerThread; j++ {
					// Select a key in a deterministic pattern
					keyIdx := (threadID*operationsPerThread + j) % keyRange
					key := fmt.Sprintf("%s-%d", keyPrefix, keyIdx)
					
					// Determine operation type (33% GET, 33% PUT, 33% DELETE)
					opType := j % 3
					
					switch opType {
					case 0: // GET
						cmd := &common.Command{
							Operation: "GET",
							Args:      []string{key},
						}
						_, err := sm.Get(cmd)
						if err != nil {
							t.Errorf("Thread %d: GET failed for key %s: %v", threadID, key, err)
						}
						
					case 1: // PUT
						value := fmt.Sprintf("mixed-value-%d-%d", threadID, j)
						cmd := &common.Command{
							Operation: "PUT",
							Args:      []string{key, value},
						}
						_, err := sm.Put(cmd)
						if err != nil {
							t.Errorf("Thread %d: PUT failed for key %s: %v", threadID, key, err)
						}
						
					case 2: // DELETE
						cmd := &common.Command{
							Operation: "DELETE",
							Args:      []string{key},
						}
						_, err := sm.Delete(cmd)
						if err != nil {
							t.Errorf("Thread %d: DELETE failed for key %s: %v", threadID, key, err)
						}
					}
				}
			}(i)
		}
		
		// Wait for all threads to finish
		wg.Wait()
		
		// Calculate elapsed time
		elapsed := time.Since(startTime)
		opsPerSecond := float64(numThreads*operationsPerThread) / elapsed.Seconds()
		
		// Report the final stage result
		size, _ := sm.Size()
		t.Logf("Completed mixed operations test with %d threads in %v", numThreads, elapsed)
		t.Logf("Throughput: %.2f ops/sec, final store size: %d", opsPerSecond, size)
	})
	
	// Get final store size
	size, err := sm.Size()
	if err != nil {
		t.Fatalf("Failed to get final store size: %v", err)
	}
	t.Logf("All comprehensive tests completed. Final store size: %d", size)
} 