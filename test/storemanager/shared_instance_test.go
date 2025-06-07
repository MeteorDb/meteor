package storemanager

import (
	"meteor/internal/common"
	"testing"
)

// TestSharedStoreManagerInstance verifies that the same StoreManager instance
// is being used across different tests and invocations
func TestSharedStoreManagerInstance(t *testing.T) {
	// Part 1: Setup - Create or retrieve the shared StoreManager
	sm1 := GetSharedStoreManager(t)
	
	// Store a unique value in the shared instance
	uniqueKey := "shared-instance-test-key"
	uniqueValue := "shared-instance-test-value"
	
	cmd := &common.Command{
		Operation: "PUT",
		Args:      []string{uniqueKey, uniqueValue},
	}
	_, err := sm1.Put(cmd)
	if err != nil {
		t.Fatalf("Failed to put test value: %v", err)
	}
	
	// Get the StoreManager instance again
	sm2 := GetSharedStoreManager(t)
	
	// Check if it's the same instance (by checking the pointer)
	if sm1 != sm2 {
		t.Errorf("Store manager instances are different: %p vs %p", sm1, sm2)
	} else {
		t.Logf("Verified that multiple calls to GetSharedStoreManager return the same instance")
	}
	
	// Verify we can read the value we set above
	getCmd := &common.Command{
		Operation: "GET",
		Args:      []string{uniqueKey},
	}
	valueBytes, err := sm2.Get(getCmd)
	if err != nil {
		t.Fatalf("Failed to get test value: %v", err)
	}
	
	retrievedValue := string(valueBytes)
	if retrievedValue != uniqueValue {
		t.Errorf("Value mismatch: expected %q, got %q", uniqueValue, retrievedValue)
	} else {
		t.Logf("Successfully verified shared StoreManager instance value persistence")
	}
	
	// Check the current store size
	size, err := sm1.Size()
	if err != nil {
		t.Fatalf("Failed to get store size: %v", err)
	}
	t.Logf("Current store size: %d", size)
	
	// Part 2: Run multiple test-like functions within this test
	t.Run("SubTestA", func(t *testing.T) {
		// Get the store manager in this subtest
		subTestSM := GetSharedStoreManager(t)
		
		// Verify it's the same instance
		if subTestSM != sm1 {
			t.Errorf("SubTestA got a different StoreManager instance: %p vs %p", subTestSM, sm1)
		}
		
		// Verify we can still access the value set in the parent test
		getCmd := &common.Command{
			Operation: "GET",
			Args:      []string{uniqueKey},
		}
		valueBytes, err := subTestSM.Get(getCmd)
		if err != nil {
			t.Fatalf("SubTestA failed to get test value: %v", err)
		}
		
		retrievedValue := string(valueBytes)
		if retrievedValue != uniqueValue {
			t.Errorf("SubTestA value mismatch: expected %q, got %q", uniqueValue, retrievedValue)
		} else {
			t.Logf("SubTestA successfully accessed the shared value")
		}
		
		// Add a new value in this subtest
		subTestKey := "subtest-a-key"
		subTestValue := "subtest-a-value"
		cmd := &common.Command{
			Operation: "PUT",
			Args:      []string{subTestKey, subTestValue},
		}
		_, err = subTestSM.Put(cmd)
		if err != nil {
			t.Fatalf("SubTestA failed to put a new value: %v", err)
		}
		t.Logf("SubTestA added a new key-value pair")
	})
	
	t.Run("SubTestB", func(t *testing.T) {
		// Get the store manager in this subtest
		subTestSM := GetSharedStoreManager(t)
		
		// Try to get the value set in SubTestA
		subTestAKey := "subtest-a-key"
		getCmd := &common.Command{
			Operation: "GET",
			Args:      []string{subTestAKey},
		}
		valueBytes, err := subTestSM.Get(getCmd)
		if err != nil {
			t.Fatalf("SubTestB failed to get SubTestA's value: %v", err)
		}
		
		retrievedValue := string(valueBytes)
		expectedValue := "subtest-a-value"
		if retrievedValue != expectedValue {
			t.Errorf("SubTestB value mismatch: expected %q, got %q", expectedValue, retrievedValue)
		} else {
			t.Logf("SubTestB successfully accessed the value set by SubTestA")
		}
		
		// Get the current store size
		size, err := subTestSM.Size()
		if err != nil {
			t.Fatalf("SubTestB failed to get store size: %v", err)
		}
		t.Logf("Final store size: %d (should include values from parent test and SubTestA)", size)
	})
} 