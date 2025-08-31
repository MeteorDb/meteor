package dbmanager

import (
	"fmt"
	"io"
	"log/slog"
	"meteor/internal/common"
	"meteor/internal/config"
	"meteor/internal/gsnmanager"
	"meteor/internal/parser"
	"meteor/internal/storemanager"
	"meteor/internal/transactionmanager"
	"meteor/internal/walmanager"
	"slices"
)

type DBManager struct {
	Parser parser.Parser
	StoreManager *storemanager.StoreManager
	GsnManager         *gsnmanager.GsnManager
	TransactionManager *transactionmanager.TransactionManager
	WalManager         *walmanager.WalManager
}

func NewDBManager() (*DBManager, error) {
	storeManager, err := storemanager.NewStoreManager()
	if err != nil {
		return nil, err
	}
	walManager, err := walmanager.NewWalManager()
	if err != nil {
		return nil, err
	}

	gsnManager, err := gsnmanager.NewGsnManager(walManager)
	if err != nil {
		return nil, err
	}

	transactionManager, err := transactionmanager.NewTransactionManager(walManager)
	if err != nil {
		return nil, err
	}

	dm := &DBManager{
		Parser: parser.NewStringParser(),
		StoreManager: storeManager,
		GsnManager: gsnManager,
		TransactionManager: transactionManager,
		WalManager: walManager,
	}

	err = dm.recoverStoreFromWal()
	if err != nil {
		return nil, err
	}

	return dm, nil
}

// This method runs 2 passes over the WAL:
// 1. First pass: read all the commit rows and their transaction ids. We only need to recover the transaction ids that were committed.
// 2. Second pass: read all the rows and put the ones to the buffer store which are part of the above list.
func (dm *DBManager) recoverStoreFromWal() error {
	activeTransactionIds := make([]uint32, 0)

	// First pass: store all the transaction ids that were committed
	err := readWalRows(dm.WalManager, func(transactionRow *common.TransactionRow) {
		fmt.Printf("transactionRow: %v\n", transactionRow)
		if transactionRow.State == common.TRANSACTION_STATE_COMMIT {
			activeTransactionIds = append(activeTransactionIds, transactionRow.TransactionId)
		}
	})

	if err != nil {
		return err
	}

	dm.WalManager.ResetOffsetToFirstRow()

	// Second pass: put the rows to the buffer store whose transaction ids are part of the above list
	err = readWalRows(dm.WalManager, func(transactionRow *common.TransactionRow) {
		transactionIdx := slices.Index(activeTransactionIds, transactionRow.TransactionId)
		if transactionIdx == -1 {
			return
		}

		// This shouldn't happen since one transaction can't be both committed and rolled back
		// But handling it just in case
		if transactionRow.State == common.TRANSACTION_STATE_ROLLBACK {
			slog.Warn("committed transaction id also has transaction rolled back state", "transactionId", transactionRow.TransactionId)
			activeTransactionIds = slices.Delete(activeTransactionIds, transactionIdx, transactionIdx + 1)
		}

		if !slices.Contains([]string{common.DB_OP_PUT, common.DB_OP_DELETE}, transactionRow.Operation) {
			return
		}

		dm.StoreManager.PutTxnRowToBufferStore(transactionRow)
	})

	if err != nil {
		return err
	}

	return nil
}

func readWalRows(walManager *walmanager.WalManager, callback func(transactionRow *common.TransactionRow)) error {
	for {
		transactionRow, err := walManager.ReadRow()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		callback(transactionRow)
	}
}

func (dm *DBManager) AddTransactionToWal(transactionRow *common.TransactionRow) error {
	if !config.Config.UseWal {
		return nil
	}
	fmt.Printf("%v\n", transactionRow)
	return dm.WalManager.AddRow(transactionRow)
}
