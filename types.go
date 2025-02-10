package database

import (
	"context"
	"net/http"
)

// DatabaseConfig съдържа конфигурацията за връзка към базата
type DatabaseConfig struct {
	Host     string // host (e.g. localhost) or path to unix domain socket directory (e.g. /private/tmp)
	Port     uint16 // default: 5432
	Database string
	User     string // default: OS user name
	Password string
	// Run-time parameters to set on connection as session default values
	// (e.g. search_path or application_name)
	RuntimeParams map[string]string
}

// Result връща резултат от заявка към базата
type Result interface {
	Scan(dest ...interface{}) (err error)

	Values() ([]interface{}, error)
}

// ResultHandler се извиква за всеки върнат ред от заявката
type ResultHandler func(Result) error

// Transaction се използва за абстракция на транзакция от базата данни
type Transaction interface {
	Commit() error
	Rollback()
	Execute(sql string, args ...interface{}) (int64, error)
	Query(query string, handler ResultHandler, args ...interface{}) (int32, error)
	Context() context.Context
}

// TransactionProvider returns Transaction to the given http.Request
type TransactionProvider func(r *http.Request) Transaction
