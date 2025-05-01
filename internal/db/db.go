package db

import (
	"meteor/internal/parser"
	"meteor/internal/storemanager"
)

type DB struct {
	Parser parser.Parser
	StoreManager *storemanager.StoreManager
}

func NewDB() (*DB, error) {
	return &DB{
		Parser: parser.NewStringParser(),
		StoreManager: &storemanager.StoreManager{},
	}, nil
}
