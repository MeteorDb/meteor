package walmanager

import (
	"fmt"
	"meteor/internal/common"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var walFileName = "meteor.wal"

type WalManager struct {
	lso atomic.Int64
	m sync.Mutex
	walFile *os.File
	walHeader *WalHeader
	walRowStartOffset int64
}

func NewWalManager() (*WalManager, error) {
	var walFile *os.File
	var walHeader *WalHeader = &WalHeader{
		Version: 1,
		NextTransactionId: 0,
		NextGsn: 0,
		Checksum: 0,
	}

	if _, err := os.Stat(walFileName); os.IsNotExist(err) {
		walFile, err = os.Create(walFileName)
		if err != nil {
			return nil, err
		}
	} else {
		walFile, err = os.OpenFile(walFileName, os.O_RDWR, 0600)
		if err != nil {
			return nil, err
		}

		_, err = common.ReadAtInFile(walFile, 0, walHeader)

		if err != nil {
			return nil, err
		}
	}

	// Rewriting the header (even if it's already written)
	newOffset, err := writeWalHeader(walFile, walHeader)
	if err != nil {
		return nil, err
	}

	walManager := &WalManager{
		lso: atomic.Int64{},
		m: sync.Mutex{},
		walFile: walFile,
		walHeader: walHeader,
	}

	walManager.walRowStartOffset = newOffset

	walManager.lso.Store(newOffset)

	return walManager, nil
}

func writeWalHeader(walFile *os.File, walHeader *WalHeader) (int64, error) {
	var newOffset int64
	if headerBytes, err := walHeader.MarshalBinary(); err != nil {
		return 0, err
	} else {
		newOffset, err = common.WriteAtInFile(walFile, 0, headerBytes)
		if err != nil {
			return 0, err
		}
	}

	return newOffset, nil
}

func (w *WalManager) AllocateTransactionIdBatch() (uint32, uint32) {
	w.m.Lock()
	defer w.m.Unlock()

	transactionId := w.walHeader.NextTransactionId
	w.walHeader.NextTransactionId += common.TRANSACTION_ID_ALLOCATION_BATCH_SIZE

	writeWalHeader(w.walFile, w.walHeader)

	return transactionId, w.walHeader.NextTransactionId
}

func (w *WalManager) AllocateGsnBatch() (uint32, uint32) {
	w.m.Lock()
	defer w.m.Unlock()

	gsn := w.walHeader.NextGsn
	w.walHeader.NextGsn += common.GSN_ALLOCATION_BATCH_SIZE

	writeWalHeader(w.walFile, w.walHeader)

	return gsn, w.walHeader.NextGsn
}

func (w *WalManager) AddRow(row *common.TransactionRow) error {
	w.m.Lock()
	defer w.m.Unlock()

	lso := w.lso.Load()

	walRow := &common.WalRow{
		Lso: lso,
		LogType: 0,
		TransactionId: row.TransactionId,
		Operation: row.Operation,
		State: row.State,
		Payload: &common.WalPayload{
			Key: row.Payload.Key,
			OldValue: row.Payload.OldValue,
			NewValue: row.Payload.NewValue,
		},
		Timestamp: time.Now().Unix(),
		Checksum: 0,
	}

	walRowBytes, err := walRow.MarshalBinary()
	if err != nil {
		return err
	}

	newOffset, err := common.WriteAtInFile(w.walFile, lso, walRowBytes)
	if err != nil {
		return err
	}

	w.lso.Store(newOffset)

	return nil
}

func (w *WalManager) ReadRow() (*common.TransactionRow, error) {
	if w.walFile == nil {
		return nil, fmt.Errorf("wal file is not loaded")
	}

	w.m.Lock()
	defer w.m.Unlock()

	lso := w.lso.Load()

	walRow := &common.WalRow{
		Payload: &common.WalPayload{
			Key: &common.K{},
			OldValue: &common.V{},
			NewValue: &common.V{},
		},
	}

	newOffset, err := common.ReadAtInFile(w.walFile, lso, walRow)
	if err != nil {
		return nil, err
	}

	w.lso.Store(newOffset)

	return &common.TransactionRow{
		TransactionId: walRow.TransactionId,
		Operation: walRow.Operation,
		State: walRow.State,
		Payload: &common.TransactionPayload{
			Key: walRow.Payload.Key,
			OldValue: walRow.Payload.OldValue,
			NewValue: walRow.Payload.NewValue,
		},
	}, nil
}

func (w *WalManager) ResetOffsetToFirstRow() {
	w.m.Lock()
	defer w.m.Unlock()

	w.lso.Store(w.walRowStartOffset)
}

func (w *WalManager) Close() error {
	return w.walFile.Close()
}