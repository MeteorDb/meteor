package parser

import (
	"meteor/internal/common"
	"net"
)

type Parser interface {
	Parse(data []byte, conn *net.Conn) (*common.Command, error)
}
