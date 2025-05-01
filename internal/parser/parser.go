package parser

import "meteor/internal/common"

type Parser interface {
	Parse(data []byte) (*common.Command, error)
}
