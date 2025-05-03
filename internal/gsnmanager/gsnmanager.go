package gsnmanager

import (
	"meteor/internal/walmanager"
	"sync"
	"sync/atomic"
)

type GsnManager struct {
	gsn atomic.Uint32
	gsnBatchStart uint32
	gsnBatchEnd uint32
	walManager *walmanager.WalManager
	m sync.Mutex
}

func NewGsnManager(walManager *walmanager.WalManager) (*GsnManager, error) {
	gsnBatchStart, gsnBatchEnd := walManager.AllocateGsnBatch()
	gsnManager := &GsnManager{
		gsn: atomic.Uint32{},
		gsnBatchStart: gsnBatchStart,
		gsnBatchEnd: gsnBatchEnd,
		walManager: walManager,
		m: sync.Mutex{},
	}

	gsnManager.gsn.Store(gsnBatchStart)

	return gsnManager, nil
}

func (gm *GsnManager) GetNewGsn() uint32 {
	if gm.gsn.Load() == gm.gsnBatchEnd - 1 {
		gm.m.Lock()
		gm.gsnBatchStart, gm.gsnBatchEnd = gm.walManager.AllocateGsnBatch()
		toReturn := gm.gsn.Add(1)
		gm.gsn.Store(gm.gsnBatchStart)
		gm.m.Unlock()
		return toReturn
	}

	return gm.gsn.Add(1)
}
