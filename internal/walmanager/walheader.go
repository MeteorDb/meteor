package walmanager

import (
	"meteor/internal/common"
)

const (
	WAL_HEADER_SIZE = 20
)

type WalHeader struct {
	Version uint32
	NextTransactionId uint32
	NextGsn uint32
	Checksum uint32
}


func (h *WalHeader) MarshalBinary() ([]byte, error) {
	bb := common.NewBinaryBuffer(WAL_HEADER_SIZE)

	bb.WriteUint32(h.Version).WriteUint32(h.NextTransactionId).WriteUint32(h.NextGsn).WriteUint32(h.Checksum)

	return bb.GetBuffer(), nil
}

func (h *WalHeader) UnmarshalBinary(data []byte) error {
	bb := common.NewBinaryBufferFrom(&data, 0)

	bb.ReadUint32(&h.Version).ReadUint32(&h.NextTransactionId).ReadUint32(&h.NextGsn).ReadUint32(&h.Checksum)

	return nil
}
