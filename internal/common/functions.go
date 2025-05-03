package common

import (
	"bytes"
	"encoding/binary"
)

func WriteToBytes(data []any) (*bytes.Buffer, error) {
	buf := &bytes.Buffer{}
	for _, d := range data {
		err := binary.Write(buf, binary.BigEndian, d)
		if err != nil {
			return nil, err
		}
	}
	return buf, nil
}

func ReadFromBytes(data any, buf *bytes.Buffer) (error) {
	return binary.Read(buf, binary.BigEndian, data)
}

