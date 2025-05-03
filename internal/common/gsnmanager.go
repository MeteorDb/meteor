package common

import "sync/atomic"

var gsn atomic.Uint32

func GetNewGsn() uint32 {
	return gsn.Add(1)
}
