package database

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	pgx "github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type txWrapper struct {
	pool  *pgxpool.Pool
	tx    pgx.Tx
	ctx   context.Context
	debug bool
}

func (t *txWrapper) Execute(sql string, args ...interface{}) (int64, error) {
	t.begin()
	tag, err := t.tx.Exec(t.ctx, sql, args...)
	if err != nil {
		log.Printf(`[tx] %s "%s"`, "Exec", sql)
		log.Printf(`[tx] %s "%s"`, "ERROR", err)
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (t *txWrapper) Query(query string, handler ResultHandler, args ...interface{}) (int32, error) {
	t.begin()
	rows, err := t.tx.Query(t.ctx, query, args...)
	if err != nil {
		return -1, err
	}
	if handler == nil {
		handler = func(r Result) error {
			return nil
		}
	}
	defer rows.Close()
	var c int32
	for rows.Next() {
		err = handler(rows)
		if err != nil {
			return -1, err
		}
		c++
	}
	return c, nil
}

func (t *txWrapper) begin() error {
	if t.tx == nil {
		tx, err := t.pool.Begin(t.ctx)
		if err != nil {
			log.Printf("[tx] %s %v\n", "START FAILED", time.Now())
			return err
		}
		t.tx = tx
	}
	return nil
}

func (t *txWrapper) Commit() error {
	if t.tx != nil {
		if t.debug {
			log.Printf("[tx] %s %v\n", "COMMITTING", time.Now())
		}
		tx := t.tx
		defer func() {
			t.tx = nil
		}()
		err := tx.Commit(t.ctx)
		if err != nil {
			t.Rollback()
			return err
		}
		if t.debug {
			log.Printf("[tx] %s %v\n", "COMMITTED", time.Now())
		}
	}
	return nil
}

func (t *txWrapper) Rollback() {
	if t.tx != nil {
		if t.debug {
			log.Printf("[tx] %s %v\n", "Rolling back...", time.Now())
		}
		tx := t.tx
		t.tx = nil
		err := tx.Rollback(t.ctx)
		if err != nil {
			log.Printf("Error rolling back: %v", err)
		}
		if t.debug {
			log.Printf("[tx] %s %v\n", "ROLLBACK", time.Now())
		}
	}
}

func (t *txWrapper) Context() context.Context {
	return t.ctx
}

type key int8

const TransactionKey key = 0

// NewTransaction връща нова транзакция
// не се препоръчва използването на тази функция в комбинация с TransactionHandler
func NewTransaction(ctx context.Context) Transaction {
	return newTransaction(ctx, false)
}

func newTransaction(ctx context.Context, debug bool) Transaction {
	if pool == nil {
		panic(errors.New("there is no database connection set"))
	}
	return &txWrapper{pool: pool, ctx: ctx, debug: debug}
}

// FromContext връща транзакцията, асоциирана с контекста. При липса на такава, връща nil
// Използването на тази функция в комбинация с TransactionHandler е ПРЕПОРЪЧИТЕЛНО
func FromContext(ctx context.Context) Transaction {
	tx, ok := ctx.Value(TransactionKey).(Transaction)
	if ok {
		return tx
	}
	panic(errors.New("unable to get Transaction. The provided request is not whapped by TransactionHandler"))
}

// TransactionHandler добавя транзакционен контекст към текугия http request
func TransactionHandler(h http.Handler) http.Handler {
	var debugEnabled bool
	if d := strings.ToLower(strings.TrimSpace(os.Getenv("DEBUG"))); d == "true" {
		debugEnabled = true
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var started time.Time
		if debugEnabled {
			log.Printf("TransactionHandler BEGIN: %v", r.URL.Path)
			started = time.Now()
		}
		ctx := r.Context()
		tx := newTransaction(ctx, debugEnabled)
		ctx = context.WithValue(ctx, TransactionKey, tx)
		defer func() {
			err := recover()
			if err != nil {
				if e, ok := err.(error); ok {
					err = errors.Unwrap(e)
				}
				pc, filename, line, _ := runtime.Caller(1)
				log.Printf("fatal error during transaction call in %s[%s:%d]: %v", runtime.FuncForPC(pc).Name(), filename, line, err)
				log.Printf("\n%s", debug.Stack())
				http.Error(w, "Internal server error", http.StatusInternalServerError)
			}
			tx.Rollback()
			if debugEnabled {
				log.Printf("TransactionHandler END: %v, Duration; %v", r.URL.Path, time.Since(started))
			}
		}()
		h.ServeHTTP(w, r.WithContext(ctx))
	})
}
