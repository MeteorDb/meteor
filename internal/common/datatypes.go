package common

type DataType uint8

const (
	TypeNull DataType = iota
	TypeUint8
	TypeUint16
	TypeUint32
	TypeUint64
	TypeInt8
	TypeInt16
	TypeInt32
	TypeInt64
	TypeBool
	TypeFloat32
	TypeFloat64
	
	TypeBytes
	TypeString
)

func (dt DataType) String() string {
	switch dt {
	case TypeNull:
		return "Null"
	case TypeUint8:
		return "Uint8"
	case TypeUint16:
		return "Uint16"
	case TypeUint32:
		return "Uint32"
	case TypeUint64:
		return "Uint64"
	case TypeInt8:
		return "Int8"
	case TypeInt16:
		return "Int16"
	case TypeInt32:
		return "Int32"
	case TypeInt64:
		return "Int64"
	case TypeBool:
		return "Bool"
	case TypeFloat32:
		return "Float32"
	case TypeFloat64:
		return "Float64"
	case TypeBytes:
		return "Bytes"
	case TypeString:
		return "String"
	default:
		return "Unknown"
	}
}

func (dt DataType) Size() int {
	switch dt {
	case TypeUint8, TypeInt8, TypeBool:
		return 1
	case TypeUint16, TypeInt16:
		return 2
	case TypeUint32, TypeInt32, TypeFloat32:
		return 4
	case TypeUint64, TypeInt64, TypeFloat64:
		return 8
	case TypeBytes, TypeString:
		return 0
	default:
		return 0
	}
}
