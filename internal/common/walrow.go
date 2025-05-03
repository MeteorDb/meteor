package common

const (
	WAL_ROW_SIZE = 32
)

type WalPayload struct {
	Key K
	OldValue V
	NewValue V
}

type WalRow struct {
	Lso int64 // Latest Sequence Offset
	LogType uint8
	TransactionId uint32
	Timestamp int64
	Operation string
	Payload WalPayload
	Checksum uint32
}

func (wp *WalPayload) MarshalBinary() ([]byte, error) {
	bb := NewBinaryBuffer(0)

	keyBytes, err := wp.Key.MarshalBinary()
	if err != nil {
		return nil, err
	}

	bb.WriteBytes(keyBytes)

	oldValueBytes, err := wp.OldValue.MarshalBinary()
	if err != nil {
		return nil, err
	}

	bb.WriteBytes(oldValueBytes)
	
	newValueBytes, err := wp.NewValue.MarshalBinary()
	if err != nil {
		return nil, err
	}

	bb.WriteBytes(newValueBytes)

	return bb.GetBuffer(), nil
}

func (wp *WalPayload) UnmarshalBinary(data []byte) error {
	bb := NewBinaryBufferFrom(&data, 0)

	keyBytes := make([]byte, 0)
	bb.ReadBytes(&keyBytes)

	err := wp.Key.UnmarshalBinary(keyBytes)
	if err != nil {
		return err
	}

	oldValueBytes := make([]byte, 0)
	bb.ReadBytes(&oldValueBytes)

	err = wp.OldValue.UnmarshalBinary(oldValueBytes)
	if err != nil {
		return err
	}

	newValueBytes := make([]byte, 0)
	bb.ReadBytes(&newValueBytes)

	err = wp.NewValue.UnmarshalBinary(newValueBytes)
	if err != nil {
		return err
	}

	return nil
}

func (wr *WalRow) MarshalBinary() ([]byte, error) {
	bb := NewBinaryBuffer(25) // 25 for the first 5 fields so number of resizing is minimal
	
	bb.WriteInt64(wr.Lso).WriteUint8(wr.LogType).WriteUint32(wr.TransactionId).WriteInt64(wr.Timestamp).WriteString(wr.Operation)

	payloadBytes, err := wr.Payload.MarshalBinary()
	if err != nil {
		return nil, err
	}

	bb.WriteBytes(payloadBytes).WriteUint32(wr.Checksum)

	return bb.GetBuffer(), nil
}

func (wr *WalRow) UnmarshalBinary(data []byte) error {
	bb := NewBinaryBufferFrom(&data, 0)

	bb.ReadInt64(&wr.Lso).ReadUint8(&wr.LogType).ReadUint32(&wr.TransactionId).ReadInt64(&wr.Timestamp).ReadString(&wr.Operation)

	payloadBytes := make([]byte, 0)
	bb.ReadBytes(&payloadBytes)

	err := wr.Payload.UnmarshalBinary(payloadBytes)
	if err != nil {
		return err
	}

	bb.ReadUint32(&wr.Checksum)

	return nil
}
