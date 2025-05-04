package common

import (
	"fmt"
	"net"
)

type K struct {
	Key string `json:"key"`
	Gsn uint32 `json:"gsn"`
}

func (k *K) String() string {
	return fmt.Sprintf("{Key: %s, Gsn: %d}", k.Key, k.Gsn)
}

type V struct {
	Type DataType `json:"type"`
	Value []byte `json:"value"`
}

func (v *V) String() string {
	return fmt.Sprintf("{Type: %d, Value: %s}", v.Type, v.Value)
}

func HashKey(key string) uint32 {
	var hash uint32
	for i := range len(key) {
		hash = 31*hash + uint32(key[i])
	}
	return hash
}

func (k *K) MarshalBinary() ([]byte, error) {
	bb := NewBinaryBuffer(0)

	bb.WriteUint32(k.Gsn).WriteString(k.Key)

	return bb.GetBuffer(), nil
}

func (k *K) UnmarshalBinary(data []byte) error {
	bb := NewBinaryBufferFrom(&data, 0)

	var gsn uint32
	var key string
	bb.ReadUint32(&gsn).ReadString(&key)

	k.Gsn = gsn
	k.Key = key

	return nil
}

func (v *V) MarshalBinary() ([]byte, error) {
	bb := NewBinaryBuffer(0)

	bb.WriteUint8(uint8(v.Type))
	bb.WriteBytes(v.Value)

	return bb.GetBuffer(), nil
}

func (v *V) UnmarshalBinary(data []byte) error {
	bb := NewBinaryBufferFrom(&data, 0)

	var dataType uint8
	valBytes := make([]byte, 0)

	bb.ReadUint8(&dataType).ReadBytes(&valBytes)
	
	v.Type = DataType(dataType)
	v.Value = valBytes

	return nil
}

type Command struct {
	Operation string   `json:"operation"`
	Args      []string `json:"args"`
	Connection *net.Conn `json:"-"`
}
