package common

const (
	WAL_ROW_SIZE = 32
)

type WalPayload struct {
	Key []byte
	OldValue []byte
	NewValue []byte
}

type WalRow struct {
	Gsn uint32 // Global Sequence Number
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

	bb.WriteBytes(wp.Key).WriteBytes(wp.OldValue).WriteBytes(wp.NewValue)

	return bb.GetBuffer(), nil
}

func (wp *WalPayload) UnmarshalBinary(data []byte) error {
	bb := NewBinaryBufferFrom(&data, 0)

	bb.ReadBytes(&wp.Key).ReadBytes(&wp.OldValue).ReadBytes(&wp.NewValue)

	return nil
}

func (wr *WalRow) MarshalBinary() ([]byte, error) {
	bb := NewBinaryBuffer(25) // 25 for the first 5 fields so number of resizing is minimal
	
	bb.WriteUint32(wr.Gsn).WriteInt64(wr.Lso).WriteUint8(wr.LogType).WriteUint32(wr.TransactionId).WriteInt64(wr.Timestamp).WriteString(wr.Operation)

	payloadBytes, err := wr.Payload.MarshalBinary()
	if err != nil {
		return nil, err
	}

	bb.WriteBytes(payloadBytes).WriteUint32(wr.Checksum)

	return bb.GetBuffer(), nil
}

func (wr *WalRow) UnmarshalBinary(data []byte) error {
	bb := NewBinaryBufferFrom(&data, 0)

	bb.ReadUint32(&wr.Gsn).ReadInt64(&wr.Lso).ReadUint8(&wr.LogType).ReadUint32(&wr.TransactionId).ReadInt64(&wr.Timestamp).ReadString(&wr.Operation)

	payloadBytes := make([]byte, 0)
	bb.ReadBytes(&payloadBytes)

	err := wr.Payload.UnmarshalBinary(payloadBytes)
	if err != nil {
		return err
	}

	bb.ReadUint32(&wr.Checksum)

	return nil
}
