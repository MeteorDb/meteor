package parser

import (
	"errors"
	"meteor/internal/common"
	"net"
	"strings"
)

type StringParser struct{}

func NewStringParser() *StringParser {
	return &StringParser{}
}

func (p *StringParser) Parse(data []byte, conn *net.Conn) (*common.Command, error) {
	s := string(data)
    parts := strings.Fields(s)
    if len(parts) == 0 {
        return nil, errors.New("invalid input")
    }

    op := strings.TrimSpace(parts[0])

    args := make([]string, len(parts)-1)
    for i, v := range parts[1:] {
        args[i] = strings.TrimSpace(v)
    }

    return &common.Command{
        Operation: op,
        Args:      args,
		Connection: conn,
    }, nil
}