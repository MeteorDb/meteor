package walmanager

import (
	"fmt"
	"io"
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
}

func NewWalManager() (*WalManager, error) {
	var walFile *os.File
	var walHeader *WalHeader = &WalHeader{
		Version: 1,
		RowStartOffset: WAL_HEADER_SIZE + 2, // 2 bytes for the header size
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
	var newOffset int64
	if headerBytes, err := walHeader.MarshalBinary(); err != nil {
		return nil, err
	} else {
		newOffset, err = common.WriteAtInFile(walFile, 0, headerBytes)
		if err != nil {
			return nil, err
		}
	}

	walManager := &WalManager{
		lso: atomic.Int64{},
		m: sync.Mutex{},
		walFile: walFile,
		walHeader: walHeader,
	}

	walManager.lso.Store(newOffset)

	return walManager, nil
}

func (w *WalManager) AddRow(row *common.TransactionRow) error {
	w.m.Lock()
	defer w.m.Unlock()

	lso := w.lso.Load()

	walRow := &common.WalRow{
		Gsn: row.Gsn,
		Lso: lso,
		LogType: 0,
		TransactionId: row.TransactionId,
		Operation: row.Operation,
		Payload: common.WalPayload{
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

	walRow := &common.WalRow{}

	newOffset, err := common.ReadAtInFile(w.walFile, lso, walRow)
	if err != nil {
		return nil, err
	}

	w.lso.Store(newOffset)

	return &common.TransactionRow{
		Gsn: walRow.Gsn,
		TransactionId: walRow.TransactionId,
		Operation: walRow.Operation,
		Payload: common.TransactionPayload(walRow.Payload),
	}, nil
}

func (w *WalManager) RecoverFromWal() error {
	for {
		transactionRow, err := w.ReadRow()
		if err != nil {
			if err == io.EOF {
				// EOF means we've reached the end of the file, which is expected
				return nil
			}
			return err
		}
		fmt.Println(transactionRow)
	}
}

func (w *WalManager) Close() error {
	return w.walFile.Close()
}