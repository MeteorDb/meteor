package parser

import (
	"fmt"
	"meteor/internal/common"
	"strconv"
	"strings"
	"unicode"
)

// TokenType represents the type of a token in the expression
type TokenType int

const (
	// Literals
	TokenField TokenType = iota
	TokenString
	TokenNumber

	// Operators
	TokenEqual
	TokenNotEqual
	TokenLess
	TokenLessEqual
	TokenGreater
	TokenGreaterEqual
	TokenLike

	// Logical operators
	TokenAnd
	TokenOr
	TokenNot

	// Delimiters
	TokenLeftParen
	TokenRightParen

	// Special
	TokenEOF
	TokenInvalid
)

// Token represents a token in the expression
type Token struct {
	Type  TokenType
	Value string
	Pos   int
}

// Lexer tokenizes condition expressions
type Lexer struct {
	input string
	pos   int
	ch    rune
}

// NewLexer creates a new lexer for the given input
func NewLexer(input string) *Lexer {
	l := &Lexer{input: input}
	l.readChar()
	return l
}

// readChar reads the next character and advances position
func (l *Lexer) readChar() {
	if l.pos >= len(l.input) {
		l.ch = 0 // ASCII NUL character represents EOF
	} else {
		l.ch = rune(l.input[l.pos])
	}
	l.pos++
}

// peekChar returns the next character without advancing position
func (l *Lexer) peekChar() rune {
	if l.pos >= len(l.input) {
		return 0
	}
	return rune(l.input[l.pos])
}

// skipWhitespace skips whitespace characters
func (l *Lexer) skipWhitespace() {
	for unicode.IsSpace(l.ch) {
		l.readChar()
	}
}

// readString reads a quoted string
func (l *Lexer) readString(quote rune) string {
	start := l.pos
	l.readChar() // skip opening quote

	for l.ch != quote && l.ch != 0 {
		l.readChar()
	}

	if l.ch == quote {
		result := l.input[start : l.pos-1]
		l.readChar() // skip closing quote
		return result
	}

	// Unterminated string, return what we have
	return l.input[start:]
}

// readIdentifier reads an identifier (field name, operator, etc.)
func (l *Lexer) readIdentifier() string {
	start := l.pos - 1

	for unicode.IsLetter(l.ch) || unicode.IsDigit(l.ch) || l.ch == '_' || l.ch == '$' {
		l.readChar()
	}

	return l.input[start : l.pos-1]
}

// readNumber reads a numeric literal
func (l *Lexer) readNumber() string {
	start := l.pos - 1

	for unicode.IsDigit(l.ch) || l.ch == '.' {
		l.readChar()
	}

	return l.input[start : l.pos-1]
}

// NextToken returns the next token from the input
func (l *Lexer) NextToken() Token {
	l.skipWhitespace()

	pos := l.pos - 1

	switch l.ch {
	case 0:
		return Token{Type: TokenEOF, Value: "", Pos: pos}
	case '(':
		l.readChar()
		return Token{Type: TokenLeftParen, Value: "(", Pos: pos}
	case ')':
		l.readChar()
		return Token{Type: TokenRightParen, Value: ")", Pos: pos}
	case '\'', '"':
		quote := l.ch
		value := l.readString(quote)
		return Token{Type: TokenString, Value: value, Pos: pos}
	case '=':
		if l.peekChar() == '=' {
			l.readChar()
			l.readChar()
			return Token{Type: TokenEqual, Value: "==", Pos: pos}
		}
		l.readChar()
		return Token{Type: TokenEqual, Value: "=", Pos: pos}
	case '!':
		if l.peekChar() == '=' {
			l.readChar()
			l.readChar()
			return Token{Type: TokenNotEqual, Value: "!=", Pos: pos}
		}
		return Token{Type: TokenInvalid, Value: "!", Pos: pos}
	case '<':
		if l.peekChar() == '=' {
			l.readChar()
			l.readChar()
			return Token{Type: TokenLessEqual, Value: "<=", Pos: pos}
		}
		l.readChar()
		return Token{Type: TokenLess, Value: "<", Pos: pos}
	case '>':
		if l.peekChar() == '=' {
			l.readChar()
			l.readChar()
			return Token{Type: TokenGreaterEqual, Value: ">=", Pos: pos}
		}
		l.readChar()
		return Token{Type: TokenGreater, Value: ">", Pos: pos}
	default:
		if unicode.IsLetter(l.ch) || l.ch == '_' || l.ch == '$' {
			identifier := l.readIdentifier()

			// Check for keywords
			switch strings.ToUpper(identifier) {
			case "AND":
				return Token{Type: TokenAnd, Value: identifier, Pos: pos}
			case "OR":
				return Token{Type: TokenOr, Value: identifier, Pos: pos}
			case "NOT":
				return Token{Type: TokenNot, Value: identifier, Pos: pos}
			case "LIKE":
				return Token{Type: TokenLike, Value: identifier, Pos: pos}
			default:
				return Token{Type: TokenField, Value: identifier, Pos: pos}
			}
		} else if unicode.IsDigit(l.ch) {
			number := l.readNumber()
			return Token{Type: TokenNumber, Value: number, Pos: pos}
		}

		// Unknown character
		ch := l.ch
		l.readChar()
		return Token{Type: TokenInvalid, Value: string(ch), Pos: pos}
	}
}

// Expression represents a parsed condition expression
type Expression interface {
	Evaluate(key string, value *common.V) bool
}

// BinaryExpression represents a binary operation
type BinaryExpression struct {
	Left     Expression
	Operator TokenType
	Right    Expression
}

func (e *BinaryExpression) Evaluate(key string, value *common.V) bool {
	switch e.Operator {
	case TokenAnd:
		return e.Left.Evaluate(key, value) && e.Right.Evaluate(key, value)
	case TokenOr:
		return e.Left.Evaluate(key, value) || e.Right.Evaluate(key, value)
	default:
		return false
	}
}

// UnaryExpression represents a unary operation (NOT)
type UnaryExpression struct {
	Operator TokenType
	Operand  Expression
}

func (e *UnaryExpression) Evaluate(key string, value *common.V) bool {
	switch e.Operator {
	case TokenNot:
		return !e.Operand.Evaluate(key, value)
	default:
		return false
	}
}

// ComparisonExpression represents a comparison operation
type ComparisonExpression struct {
	Field    string
	Operator TokenType
	Value    string
}

func (e *ComparisonExpression) Evaluate(key string, value *common.V) bool {
	if value == nil || value.Type == common.TypeTombstone {
		return false
	}

	var leftValue string
	switch e.Field {
	case "$key", "key", "key_name":
		leftValue = key
	case "$value", "value":
		leftValue = string(value.Value)
	default:
		// TODO: Currently treating unknown fields as key comparisons which is wrong. Example: returns key a for (a=a) comparison. We need to throw error here.
		leftValue = key
	}

	rightValue := e.Value

	switch e.Operator {
	case TokenEqual:
		return leftValue == rightValue
	case TokenNotEqual:
		return leftValue != rightValue
	case TokenLess:
		// Try numeric comparison first
		if leftNum, err := strconv.ParseFloat(leftValue, 64); err == nil {
			if rightNum, err := strconv.ParseFloat(rightValue, 64); err == nil {
				return leftNum < rightNum
			}
		}
		return leftValue < rightValue
	case TokenLessEqual:
		if leftNum, err := strconv.ParseFloat(leftValue, 64); err == nil {
			if rightNum, err := strconv.ParseFloat(rightValue, 64); err == nil {
				return leftNum <= rightNum
			}
		}
		return leftValue <= rightValue
	case TokenGreater:
		if leftNum, err := strconv.ParseFloat(leftValue, 64); err == nil {
			if rightNum, err := strconv.ParseFloat(rightValue, 64); err == nil {
				return leftNum > rightNum
			}
		}
		return leftValue > rightValue
	case TokenGreaterEqual:
		if leftNum, err := strconv.ParseFloat(leftValue, 64); err == nil {
			if rightNum, err := strconv.ParseFloat(rightValue, 64); err == nil {
				return leftNum >= rightNum
			}
		}
		return leftValue >= rightValue
	case TokenLike:
		// Simple LIKE implementation with % wildcards
		if strings.HasSuffix(rightValue, "%") && strings.HasPrefix(rightValue, "%") {
			// %value% - contains
			substring := strings.Trim(rightValue, "%")
			return strings.Contains(leftValue, substring)
		} else if strings.HasSuffix(rightValue, "%") {
			// value% - prefix
			prefix := strings.TrimSuffix(rightValue, "%")
			return strings.HasPrefix(leftValue, prefix)
		} else if strings.HasPrefix(rightValue, "%") {
			// %value - suffix
			suffix := strings.TrimPrefix(rightValue, "%")
			return strings.HasSuffix(leftValue, suffix)
		} else {
			// No wildcards - exact match
			return leftValue == rightValue
		}
	default:
		return false
	}
}

// ConditionParser parses condition expressions
type ConditionParser struct {
	lexer        *Lexer
	currentToken Token
	peekToken    Token
}

// NewConditionParser creates a new condition parser
func NewConditionParser(input string) *ConditionParser {
	// Remove WHERE prefix if present (for backward compatibility)
	input = strings.TrimSpace(input)
	if strings.HasPrefix(strings.ToUpper(input), "WHERE ") {
		input = strings.TrimSpace(input[6:])
	}

	p := &ConditionParser{lexer: NewLexer(input)}
	p.nextToken()
	p.nextToken()
	return p
}

// nextToken advances the parser to the next token
func (p *ConditionParser) nextToken() {
	p.currentToken = p.peekToken
	p.peekToken = p.lexer.NextToken()
}

// ParseExpression parses a condition expression and returns a filter function
func (p *ConditionParser) ParseExpression() (func(string, *common.V) bool, error) {
	expr, err := p.parseOrExpression()
	if err != nil {
		return nil, err
	}

	if p.currentToken.Type != TokenEOF {
		return nil, fmt.Errorf("unexpected token: %s at position %d", p.currentToken.Value, p.currentToken.Pos)
	}

	return func(key string, value *common.V) bool {
		return expr.Evaluate(key, value)
	}, nil
}

// parseOrExpression parses OR expressions (lowest precedence)
func (p *ConditionParser) parseOrExpression() (Expression, error) {
	left, err := p.parseAndExpression()
	if err != nil {
		return nil, err
	}

	for p.currentToken.Type == TokenOr {
		operator := p.currentToken.Type
		p.nextToken()

		right, err := p.parseAndExpression()
		if err != nil {
			return nil, err
		}

		left = &BinaryExpression{
			Left:     left,
			Operator: operator,
			Right:    right,
		}
	}

	return left, nil
}

// parseAndExpression parses AND expressions
func (p *ConditionParser) parseAndExpression() (Expression, error) {
	left, err := p.parseNotExpression()
	if err != nil {
		return nil, err
	}

	for p.currentToken.Type == TokenAnd {
		operator := p.currentToken.Type
		p.nextToken()

		right, err := p.parseNotExpression()
		if err != nil {
			return nil, err
		}

		left = &BinaryExpression{
			Left:     left,
			Operator: operator,
			Right:    right,
		}
	}

	return left, nil
}

// parseNotExpression parses NOT expressions
func (p *ConditionParser) parseNotExpression() (Expression, error) {
	if p.currentToken.Type == TokenNot {
		operator := p.currentToken.Type
		p.nextToken()

		operand, err := p.parseNotExpression()
		if err != nil {
			return nil, err
		}

		return &UnaryExpression{
			Operator: operator,
			Operand:  operand,
		}, nil
	}

	return p.parseComparisonExpression()
}

// parseComparisonExpression parses comparison expressions (highest precedence)
func (p *ConditionParser) parseComparisonExpression() (Expression, error) {
	if p.currentToken.Type == TokenLeftParen {
		p.nextToken()
		expr, err := p.parseOrExpression()
		if err != nil {
			return nil, err
		}

		if p.currentToken.Type != TokenRightParen {
			return nil, fmt.Errorf("expected ')' at position %d", p.currentToken.Pos)
		}
		p.nextToken()

		return expr, nil
	}

	// Parse field operator value or value operator field
	if p.currentToken.Type != TokenField && p.currentToken.Type != TokenString && p.currentToken.Type != TokenNumber {
		return nil, fmt.Errorf("expected field, string, or number at position %d", p.currentToken.Pos)
	}

	leftToken := p.currentToken
	p.nextToken()

	if !isComparisonOperator(p.currentToken.Type) {
		return nil, fmt.Errorf("expected comparison operator at position %d", p.currentToken.Pos)
	}

	operator := p.currentToken.Type
	p.nextToken()

	if p.currentToken.Type != TokenField && p.currentToken.Type != TokenString && p.currentToken.Type != TokenNumber {
		return nil, fmt.Errorf("expected field, string, or number at position %d", p.currentToken.Pos)
	}

	rightToken := p.currentToken
	p.nextToken()

	// Determine which is the field and which is the value
	var field, value string

	// Handle both directions: field op value AND value op field
	if isFieldToken(leftToken) && !isFieldToken(rightToken) {
		// Normal: field op value
		field = leftToken.Value
		value = rightToken.Value
	} else if !isFieldToken(leftToken) && isFieldToken(rightToken) {
		// Reverse: value op field
		field = rightToken.Value
		value = leftToken.Value
		// Reverse the operator for correct evaluation
		operator = reverseOperator(operator)
	} else if isFieldToken(leftToken) && isFieldToken(rightToken) {
		// Both are fields - treat left as field, right as value
		field = leftToken.Value
		value = rightToken.Value
	} else {
		// Both are values - invalid
		return nil, fmt.Errorf("comparison requires at least one field reference")
	}

	return &ComparisonExpression{
		Field:    field,
		Operator: operator,
		Value:    value,
	}, nil
}

// isComparisonOperator checks if the token is a comparison operator
func isComparisonOperator(tokenType TokenType) bool {
	switch tokenType {
	case TokenEqual, TokenNotEqual, TokenLess, TokenLessEqual, TokenGreater, TokenGreaterEqual, TokenLike:
		return true
	default:
		return false
	}
}

// isFieldToken checks if the token represents a field
func isFieldToken(token Token) bool {
	if token.Type == TokenField {
		return true
	}
	// Strings can also be field names if they start with $
	if token.Type == TokenString && strings.HasPrefix(token.Value, "$") {
		return true
	}
	return false
}

// reverseOperator reverses comparison operators for reverse expressions
func reverseOperator(op TokenType) TokenType {
	switch op {
	case TokenLess:
		return TokenGreater
	case TokenLessEqual:
		return TokenGreaterEqual
	case TokenGreater:
		return TokenLess
	case TokenGreaterEqual:
		return TokenLessEqual
	default:
		return op // Equal, NotEqual, Like remain the same
	}
}
