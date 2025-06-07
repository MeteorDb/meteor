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
	storeManager, err := storemanager.NewStoreManager()
	if err != nil {
		return nil, err
	}
	err = storeManager.RecoverStoreFromWal()
	if err != nil {
		return nil, err
	}
	return &DB{
		Parser: parser.NewStringParser(),
		StoreManager: storeManager,
	}, nil
}
