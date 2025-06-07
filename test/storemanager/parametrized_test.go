package storemanager

import (
	"fmt"
	"meteor/internal/common"
	"meteor/internal/storemanager"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// TestParametrizedConcurrency tests the StoreManager with different numbers of threads
// to see how it performs with different levels of concurrency.
func TestParametrizedConcurrency(t *testing.T) {
	// Test with different numbers of threads
	threadCounts := []int{1, 2, 4, 8, 16, 32, 64}
	
	// Get the shared StoreManager
	sm := GetSharedStoreManager(t)
	
	for _, numThreads := range threadCounts {
		t.Run(fmt.Sprintf("Threads=%d", numThreads), func(t *testing.T) {
			// Reset the store manager for each test run
			ResetSharedStoreManager(t)
			
			// Run the concurrent increments test with the specified number of threads
			runConcurrentIncrements(t, sm, numThreads)
		})
	}
}

// runConcurrentIncrements runs a test where multiple threads increment counters concurrently
func runConcurrentIncrements(t *testing.T, sm *storemanager.StoreManager, numThreads int) {
	// Test parameters
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
	
	// Create array to track expected increments
	expectedIncrements := make([]int, numCounters)
	
	// Start all goroutines
	for i := 0; i < numThreads; i++ {
		threadID := i
		go func() {
			defer wg.Done()
			
			// Each thread increments random counters
			for j := 0; j < numIncrements; j++ {
				// Select a counter (deterministically based on thread ID and iteration)
				counterIdx := (threadID*numIncrements + j) % numCounters
				key := fmt.Sprintf("counter-%d", counterIdx)
				
				// Get current value
				getCmd := &common.Command{
					Operation: "GET",
					Args:      []string{key},
				}
				valueBytes, err := sm.Get(getCmd)
				if err != nil {
					t.Errorf("Thread %d: GET failed for counter %s: %v", threadID, key, err)
					continue
				}
				
				// Parse and increment value
				currentValue, err := strconv.Atoi(string(valueBytes))
				if err != nil {
					t.Errorf("Thread %d: Failed to parse value %s: %v", threadID, string(valueBytes), err)
					continue
				}
				newValue := currentValue + 1
				
				// Store the new value
				putCmd := &common.Command{
					Operation: "PUT",
					Args:      []string{key, strconv.Itoa(newValue)},
				}
				_, err = sm.Put(putCmd)
				if err != nil {
					t.Errorf("Thread %d: PUT failed for counter %s: %v", threadID, key, err)
					continue
				}
				
				// Track the increment for verification
				expectedIncrements[counterIdx]++
			}
		}()
	}
	
	// Wait for all goroutines to complete
	wg.Wait()
	
	// Verify final counter values
	raceConditionsDetected := 0
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
		
		if finalValue != expectedValue {
			t.Errorf("Race condition detected for counter %s: expected %d increments, got %d", 
				key, expectedValue, finalValue)
			raceConditionsDetected++
		}
	}
	
	if raceConditionsDetected > 0 {
		t.Errorf("Detected %d race conditions with %d threads", raceConditionsDetected, numThreads)
	} else {
		t.Logf("No race conditions detected with %d threads", numThreads)
	}
}

// TestHighContentionOperations tests operations with extremely high contention
// by having all threads operate on the same key.
func TestHighContentionOperations(t *testing.T) {
	// Get the shared StoreManager and reset it
	sm := GetSharedStoreManager(t)
	ResetSharedStoreManager(t)
	
	// Define test parameters
	numThreads := 20
	numOperations := 50
	key := "high-contention-key"
	
	// Initialize key with an empty string
	cmd := &common.Command{
		Operation: "PUT",
		Args:      []string{key, ""},
	}
	_, err := sm.Put(cmd)
	if err != nil {
		t.Fatalf("Failed to initialize key %s: %v", key, err)
	}
	
	// Create a wait group to wait for all goroutines to finish
	var wg sync.WaitGroup
	wg.Add(numThreads)
	
	// Create array to track expected tokens
	expectedTokens := make([]string, 0, numThreads*numOperations)
	var tokensMutex sync.Mutex
	
	// Start all goroutines
	for i := 0; i < numThreads; i++ {
		threadID := i
		go func() {
			defer wg.Done()
			
			// Each thread appends to the same string multiple times
			for j := 0; j < numOperations; j++ {
				// Generate a unique token to append
				token := fmt.Sprintf("[%d-%d]", threadID, j)
				
				// Critical section - get, modify, put
				getCmd := &common.Command{
					Operation: "GET",
					Args:      []string{key},
				}
				valueBytes, err := sm.Get(getCmd)
				if err != nil {
					t.Errorf("Thread %d: GET failed for key %s: %v", threadID, key, err)
					continue
				}
				
				currentValue := string(valueBytes)
				newValue := currentValue + token
				
				putCmd := &common.Command{
					Operation: "PUT",
					Args:      []string{key, newValue},
				}
				_, err = sm.Put(putCmd)
				if err != nil {
					t.Errorf("Thread %d: PUT failed for key %s: %v", threadID, key, err)
					continue
				}
				
				// Track token for verification
				tokensMutex.Lock()
				expectedTokens = append(expectedTokens, token)
				tokensMutex.Unlock()
			}
		}()
	}
	
	// Wait for all goroutines to complete
	wg.Wait()
	
	// Verify the final string
	getCmd := &common.Command{
		Operation: "GET",
		Args:      []string{key},
	}
	valueBytes, err := sm.Get(getCmd)
	if err != nil {
		t.Fatalf("Final GET failed for key %s: %v", key, err)
	}
	
	finalValue := string(valueBytes)
	
	// Verify all tokens are present
	missingTokens := 0
	for _, token := range expectedTokens {
		if !strings.Contains(finalValue, token) {
			missingTokens++
		}
	}
	
	// Expected number of tokens
	expectedTokenCount := numThreads * numOperations
	
	t.Logf("Final string length: %d bytes", len(finalValue))
	t.Logf("Total expected tokens: %d", expectedTokenCount)
	
	// Calculate the total expected length
	expectedLength := 0
	for _, token := range expectedTokens {
		expectedLength += len(token)
	}
	
	if missingTokens > 0 {
		t.Errorf("Missing %d tokens out of %d total tokens", missingTokens, expectedTokenCount)
	} else if len(finalValue) != expectedLength {
		t.Errorf("String length mismatch: expected %d, got %d", expectedLength, len(finalValue))
	} else {
		t.Logf("All tokens present and string length matches exactly!")
	}
} 