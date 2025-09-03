package commands

import (
	"meteor/internal/common"
	"meteor/internal/dbmanager"
	"net"
)

// addReadValueToTxnStore adds the key value pair to transaction store so future reads return the same value
func addReadValueToTxnStore(dm *dbmanager.DBManager, transactionId uint32, key string, value *common.V, isolationLevel string, conn *net.Conn) error {
	// Store read value in transaction store for REPEATABLE_READ and SNAPSHOT_ISOLATION
	// So following queries return the same value even if the value is changed by other transactions
	if isolationLevel == common.TXN_ISOLATION_REPEATABLE_READ ||
		isolationLevel == common.TXN_ISOLATION_SNAPSHOT_ISOLATION ||
		isolationLevel == common.TXN_ISOLATION_SERIALIZABLE {
		var gsn uint32
		if isolationLevel == common.TXN_ISOLATION_REPEATABLE_READ || isolationLevel == common.TXN_ISOLATION_SERIALIZABLE {
			gsn, _ = dm.StoreManager.BufferStore.GetLatestGsn(key)
		} else {
			// TODO: This is incorrect, the gsn should be <= start gsn and not start gsn itself. Though it shouldn't cause a problem as this gsn is temporarily stored in transaction store only to serve same value for future reads.
			gsn, _ = dm.TransactionManager.GetTransactionStartGsn(transactionId)
		}
		keyObj := &common.K{Key: key, Gsn: gsn}
		// Only adding to transaction store so consecutive reads return the same value. No relation to commits.
		transactionRow := common.NewTransactionRow(transactionId, common.DB_OP_GET, common.TRANSACTION_STATE_QUEUED, keyObj, nil, value)
		err := dm.TransactionManager.AddTransaction(transactionRow, conn)
		if err != nil {
			return err
		}
		_ = dm.AddTransactionToWal(transactionRow) // Optional
	}
	return nil
}

// addReadValuesToTxnStoreByAcquiringLocks adds multiple read key value pairs to transaction store by acquiring read lock for each key based for repeatable read isolation
func addReadValuesToTxnStoreByAcquiringLocks(dm *dbmanager.DBManager, transactionId uint32, results map[string]*common.V, isolationLevel string, conn *net.Conn) error {
	for key, value := range results {
		if isolationLevel == common.TXN_ISOLATION_REPEATABLE_READ {
			err := dm.TransactionManager.AcquireReadLock(transactionId, key, isolationLevel)
			if err != nil {
				return err
			}
		}

		err := addReadValueToTxnStore(dm, transactionId, key, value, isolationLevel, conn)
		if err != nil {
			return err
		}
	}
	return nil
}
