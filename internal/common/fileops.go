package common

import (
	"os"
)

func ReadUint16FromFile(file *os.File, offset int64) (uint16, error) {
	firstTwoBytes := make([]byte, 2)
	_, err := file.ReadAt(firstTwoBytes, 0)
	if err != nil {
		return 0, err
	}

	var val uint16
	NewBinaryBufferFrom(&firstTwoBytes, 0).ReadUint16(&val)

	return val, nil
}

func WriteAtInFile(file *os.File, offset int64, data []byte) (int64, error) {
	dataSizeBytes := NewBinaryBuffer(2).WriteUint16(uint16(len(data))).GetBuffer()
	_, err := file.WriteAt(dataSizeBytes, offset)
	if err != nil {
		return -1, err
	}
	_, err = file.WriteAt(data, offset + 2)
	return offset + 2 + int64(len(data)), err
}

func ReadAtInFile(file *os.File, offset int64, payload BinarySerializable) (int64, error) {
	dataSizeBytes := make([]byte, 2)
	_, err := file.ReadAt(dataSizeBytes, offset)
	if err != nil {
		return -1, err
	}

	var dataSize uint16
	NewBinaryBufferFrom(&dataSizeBytes, 0).ReadUint16(&dataSize)

	dataBytes := make([]byte, dataSize)
	_, err = file.ReadAt(dataBytes, offset + 2)
	if err != nil {
		return -1, err
	}

	err = payload.UnmarshalBinary(dataBytes)

	return offset + 2 + int64(len(dataBytes)), err
}
