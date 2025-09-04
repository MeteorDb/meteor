package parser

import (
	"errors"
	"log"
	"meteor/internal/common"
	"net"
	"strings"
)

type StringParser struct{}

func NewStringParser() *StringParser {
	return &StringParser{}
}

func (p *StringParser) Parse(data []byte, conn *net.Conn) (*common.Command, error) {
	s := strings.TrimSpace(string(data))
    parts := parseQuotedArgs(s)
    if len(parts) == 0 {
        return nil, errors.New("invalid input")
    }

    op := strings.TrimSpace(parts[0])

    args := make([]string, len(parts)-1)
    for i, v := range parts[1:] {
        args[i] = v
    }

    return &common.Command{
        Operation: op,
        Args:      args,
		Connection: conn,
    }, nil
}

// parseQuotedArgs parses a string into arguments, respecting single and double quotes
// Example: `a b c` -> ['a','b','c']
// Example: `"a b" 'c d'"e f"'g h'` -> ['a b', 'c d', 'e f', 'g h']
func parseQuotedArgs(input string) []string {
	var args []string
	var current strings.Builder
	var inSingleQuote, inDoubleQuote bool = false, false
	
	runes := []rune(input)
	for i := 0; i < len(runes); i++ {
		char := runes[i]
		
		switch char {
		case '\'':
			log.Println("inSingleQuote", inSingleQuote)
			if !inDoubleQuote {
				inSingleQuote = !inSingleQuote
			} else {
				current.WriteRune(char)
			}
		case '"':
			log.Println("inDoubleQuote", inDoubleQuote)
			if !inSingleQuote {
				inDoubleQuote = !inDoubleQuote
			} else {
				current.WriteRune(char)
			}
		case ' ', '\t', '\n', '\r':
			log.Println("inSingleQuote", inSingleQuote, "inDoubleQuote", inDoubleQuote)
			if inSingleQuote || inDoubleQuote {
				current.WriteRune(char)
			} else {
				// End of current argument
				if current.Len() > 0 {
					args = append(args, current.String())
					current.Reset()
				}
				// Skip consecutive whitespace
				for i+1 < len(runes) && isWhitespace(runes[i+1]) {
					i++
				}
			}
		default:
			log.Println("default", char)
			current.WriteRune(char)
		}
	}
	
	// Add the last argument if there's any content
	if current.Len() > 0 {
		args = append(args, current.String())
	}
	
	return args
}

func isWhitespace(r rune) bool {
	return r == ' ' || r == '\t' || r == '\n' || r == '\r'
}