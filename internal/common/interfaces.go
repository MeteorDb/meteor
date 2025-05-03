package common

type BinarySerializable interface {
    MarshalBinary() ([]byte, error)
    UnmarshalBinary([]byte) error
}
