package storemanager

import (
	"fmt"
	"meteor/internal/common"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrentOperations validates that the StoreManager
// is thread-safe by running multiple goroutines that perform
// GET, PUT, DELETE operations concurrently.
func TestConcurrentOperations(t *testing.T) {
	// Get the shared StoreManager and reset it
	sm := GetSharedStoreManager(t)
	ResetSharedStoreManager(t)

	// Define test parameters
	numThreads := 10
	numOperationsPerThread := 1000
	keyRange := 100 // Use a limited key range to increase contention

	// Create a wait group to wait for all goroutines to finish
	var wg sync.WaitGroup
	wg.Add(numThreads)

	// Create a map to track expected final values
	expectedValues := sync.Map{}
	
	// Use atomic counter to track successful operations
	var opsCounter atomic.Int64

	// Start N goroutines
	for i := 0; i < numThreads; i++ {
		threadID := i
		go func() {
			defer wg.Done()

			// Each thread performs a mix of operations
			for j := 0; j < numOperationsPerThread; j++ {
				// Select a random key within the range
				keyNum := (threadID*numOperationsPerThread + j) % keyRange
				key := fmt.Sprintf("key-%d", keyNum)

				// Perform a random operation (33% GET, 33% PUT, 33% DELETE)
				opType := j % 3
				
				switch opType {
				case 0: // GET
					cmd := &common.Command{
						Operation: "GET",
						Args:      []string{key},
					}
					_, err := sm.Get(cmd)
					if err != nil {
						t.Errorf("Thread %d: GET operation failed for key %s: %v", threadID, key, err)
					} else {
						opsCounter.Add(1)
					}

				case 1: // PUT
					value := fmt.Sprintf("value-%d-%d", threadID, j)
					cmd := &common.Command{
						Operation: "PUT",
						Args:      []string{key, value},
					}
					_, err := sm.Put(cmd)
					if err != nil {
						t.Errorf("Thread %d: PUT operation failed for key %s: %v", threadID, key, err)
					} else {
						// Update expected value
						expectedValues.Store(key, value)
						opsCounter.Add(1)
					}

				case 2: // DELETE
					cmd := &common.Command{
						Operation: "DELETE",
						Args:      []string{key},
					}
					_, err := sm.Delete(cmd)
					if err != nil {
						t.Errorf("Thread %d: DELETE operation failed for key %s: %v", threadID, key, err)
					} else {
						// Update expected value to indicate deletion
						expectedValues.Delete(key)
						opsCounter.Add(1)
					}
				}
			}
		}()
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Verify the final state
	t.Logf("Completed %d operations across %d threads", opsCounter.Load(), numThreads)
	
	// Validate all expected values
	inconsistencies := 0
	expectedValues.Range(func(keyInterface, valueInterface interface{}) bool {
		key := keyInterface.(string)
		expectedValue := valueInterface.(string)
		
		// Get the actual value
		cmd := &common.Command{
			Operation: "GET",
			Args:      []string{key},
		}
		actualValueBytes, err := sm.Get(cmd)
		if err != nil {
			t.Errorf("Validation GET failed for key %s: %v", key, err)
			inconsistencies++
			return true
		}
		
		actualValue := string(actualValueBytes)
		if actualValue != expectedValue {
			t.Errorf("Inconsistency detected for key %s: expected %s, got %s", 
				key, expectedValue, actualValue)
			inconsistencies++
		}
		return true
	})
	
	// Also check for keys that should be deleted
	size, err := sm.Size()
	if err != nil {
		t.Fatalf("Failed to get store size: %v", err)
	}
	
	// Count expected size
	expectedSize := 0
	expectedValues.Range(func(_, _ interface{}) bool {
		expectedSize++
		return true
	})
	
	if size != expectedSize {
		t.Errorf("Store size mismatch: expected %d, got %d", expectedSize, size)
	}
	
	if inconsistencies > 0 {
		t.Errorf("Found %d data inconsistencies", inconsistencies)
	} else {
		t.Logf("No data inconsistencies found after concurrent operations")
	}
}

// TestIncrementOperations tests a specific scenario where multiple threads
// try to increment counter values concurrently, which requires GET-then-PUT
// operations, making it prone to race conditions.
func TestIncrementOperations(t *testing.T) {
	// Get the shared StoreManager and reset it
	sm := GetSharedStoreManager(t)
	ResetSharedStoreManager(t)

	// Define test parameters
	numThreads := 10
	numIncrements := 100
	numCounters := 5 // Small number of counters to increase contention

	// Initialize counters to 0
	for i := 0; i < numCounters; i++ {
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

	// Create a wait group to wait for all goroutines to finish
	var wg sync.WaitGroup
	wg.Add(numThreads)

	// Create array to track how many increments each counter should have received
	expectedIncrements := make([]int, numCounters)

	// Start time
	startTime := time.Now()

	// Start N goroutines
	for i := 0; i < numThreads; i++ {
		threadID := i
		go func() {
			defer wg.Done()

			// Each thread increments random counters
			for j := 0; j < numIncrements; j++ {
				// Select a random counter
				counterIdx := (threadID*numIncrements + j) % numCounters
				key := fmt.Sprintf("counter-%d", counterIdx)

				// This is a critical section that needs proper synchronization
				// in the StoreManager to work correctly:
				// 1. Get current value
				// 2. Increment it
				// 3. Store back the new value
				
				// Step 1: Get current value
				getCmd := &common.Command{
					Operation: "GET",
					Args:      []string{key},
				}
				valueBytes, err := sm.Get(getCmd)
				if err != nil {
					t.Errorf("Thread %d: GET failed for counter %s: %v", threadID, key, err)
					continue
				}
				
				// Step 2: Parse and increment value
				currentValue, err := strconv.Atoi(string(valueBytes))
				if err != nil {
					t.Errorf("Thread %d: Failed to parse value %s: %v", threadID, string(valueBytes), err)
					continue
				}
				newValue := currentValue + 1
				
				// Step 3: Store the new value
				putCmd := &common.Command{
					Operation: "PUT",
					Args:      []string{key, strconv.Itoa(newValue)},
				}
				_, err = sm.Put(putCmd)
				if err != nil {
					t.Errorf("Thread %d: PUT failed for counter %s: %v", threadID, key, err)
					continue
				}
				
				// Track expected increments for verification
				expectedIncrements[counterIdx]++
			}
		}()
	}

	// Wait for all goroutines to complete
	wg.Wait()
	
	// End time
	duration := time.Since(startTime)
	t.Logf("All increment operations completed in %v", duration)

	// Verify final counter values
	for i := 0; i < numCounters; i++ {
		key := fmt.Sprintf("counter-%d", i)
		getCmd := &common.Command{
			Operation: "GET",
			Args:      []string{key},
		}
		valueBytes, err := sm.Get(getCmd)
		if err != nil {
			t.Errorf("Validation GET failed for counter %s: %v", key, err)
			continue
		}
		
		finalValue, err := strconv.Atoi(string(valueBytes))
		if err != nil {
			t.Errorf("Failed to parse final value %s: %v", string(valueBytes), err)
			continue
		}
		
		expectedValue := expectedIncrements[i]
		
		// The final value should be exactly equal to the number of increments
		// If not, it means there were race conditions
		if finalValue != expectedValue {
			t.Errorf("Race condition detected for counter %s: expected %d increments, got %d", 
				key, expectedValue, finalValue)
		} else {
			t.Logf("Counter %s: %d increments applied correctly", key, expectedValue)
		}
	}
}

// TestStringManipulationOperations tests concurrent string append operations,
// which requires GET-then-PUT operations and is prone to race conditions.
func TestStringManipulationOperations(t *testing.T) {
	// Get the shared StoreManager and reset it
	sm := GetSharedStoreManager(t)
	ResetSharedStoreManager(t)

	// Define test parameters
	numThreads := 10
	numOperations := 50
	numStrings := 3 // Small number to increase contention

	// Initialize strings
	for i := 0; i < numStrings; i++ {
		key := fmt.Sprintf("string-%d", i)
		cmd := &common.Command{
			Operation: "PUT",
			Args:      []string{key, ""},
		}
		_, err := sm.Put(cmd)
		if err != nil {
			t.Fatalf("Failed to initialize string %s: %v", key, err)
		}
	}

	// Create a wait group to wait for all goroutines to finish
	var wg sync.WaitGroup
	wg.Add(numThreads)

	// Create array to track expected string values
	expectedAppends := make([][]string, numStrings)
	for i := range expectedAppends {
		expectedAppends[i] = make([]string, 0, numThreads*numOperations)
	}

	// Start N goroutines
	for i := 0; i < numThreads; i++ {
		threadID := i
		go func() {
			defer wg.Done()

			// Each thread appends to random strings
			for j := 0; j < numOperations; j++ {
				// Select a string to modify
				stringIdx := (threadID*numOperations + j) % numStrings
				key := fmt.Sprintf("string-%d", stringIdx)
				
				// Generate a unique token to append
				token := fmt.Sprintf("[%d-%d]", threadID, j)
				
				// This is a critical section:
				// 1. Get current string
				// 2. Append token
				// 3. Store back the new string
				
				// Step 1: Get current string
				getCmd := &common.Command{
					Operation: "GET",
					Args:      []string{key},
				}
				valueBytes, err := sm.Get(getCmd)
				if err != nil {
					t.Errorf("Thread %d: GET failed for string %s: %v", threadID, key, err)
					continue
				}
				
				// Step 2: Append token
				currentValue := string(valueBytes)
				newValue := currentValue + token
				
				// Step 3: Store the new string
				putCmd := &common.Command{
					Operation: "PUT",
					Args:      []string{key, newValue},
				}
				_, err = sm.Put(putCmd)
				if err != nil {
					t.Errorf("Thread %d: PUT failed for string %s: %v", threadID, key, err)
					continue
				}
				
				// Track what we appended for verification
				expectedAppends[stringIdx] = append(expectedAppends[stringIdx], token)
			}
		}()
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Verify final string values
	for i := 0; i < numStrings; i++ {
		key := fmt.Sprintf("string-%d", i)
		getCmd := &common.Command{
			Operation: "GET",
			Args:      []string{key},
		}
		valueBytes, err := sm.Get(getCmd)
		if err != nil {
			t.Errorf("Validation GET failed for string %s: %v", key, err)
			continue
		}
		
		finalValue := string(valueBytes)
		
		// Check if all tokens are present in the final string
		allTokensFound := true
		for _, token := range expectedAppends[i] {
			if !containsToken(finalValue, token) {
				allTokensFound = false
				t.Errorf("Missing token %s in string %s", token, key)
			}
		}
		
		// All tokens should be present, and the string length should match the sum of token lengths
		expectedLength := 0
		for _, token := range expectedAppends[i] {
			expectedLength += len(token)
		}
		
		if len(finalValue) != expectedLength {
			t.Errorf("String length mismatch for %s: expected %d, got %d", 
				key, expectedLength, len(finalValue))
		}
		
		if allTokensFound && len(finalValue) == expectedLength {
			t.Logf("String %s: all %d tokens appended correctly", key, len(expectedAppends[i]))
		}
	}
}

// Helper function to check if a string contains a token
func containsToken(s, token string) bool {
	return strings.Contains(s, token)
} 