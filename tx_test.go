package database_test

import (
	"context"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mmitevski/database"
)

func databaseSetup(t *testing.T) {
	log.Printf("Executing database setup...")
	defer log.Printf("Database setup done.")
	database.ConfigureFromString("host=localhost database=test user=test password=test statement_cache_capacity=0")
	const sql = `
	drop table if exists category;
	CREATE TABLE public.category (
		id BIGSERIAL NOT NULL,
		name TEXT NOT NULL,
		featured BOOLEAN DEFAULT false NOT NULL,
		CONSTRAINT category_pkey PRIMARY KEY(id)
	  ) ;

	  CREATE INDEX category_name ON public.category
		USING btree (featured, (upper(name)) COLLATE pg_catalog."default");
		  `
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	tx := database.NewTransaction(ctx)
	if _, err := tx.Execute(sql); err != nil {
		t.Errorf("Error setting up database: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Errorf("Error committing setting up database transaction: %v", err)
	}
}

func databaseClean(t *testing.T) {
	log.Printf("Executing database clean...")
	defer log.Printf("Database clean done.")
	const sql = `drop table category`
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	tx := database.NewTransaction(ctx)
	if _, err := tx.Execute(sql); err != nil {
		t.Errorf("Error cleaning database: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Errorf("Error committing cleaning database transaction: %v", err)
	}
}

type testHandler struct {
	t     *testing.T
	serve http.HandlerFunc
}

func (th *testHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	th.serve(w, r)
}

func TestTransactionHandler(t *testing.T) {
	databaseSetup(t)
	defer databaseClean(t)
	var th testHandler
	th.t = t
	{
		th.serve = func(rw http.ResponseWriter, r *http.Request) {
			th.t.Logf("testHandler BEGIN")
			tx := database.FromContext(r.Context())
			if _, err := tx.Execute("insert into category(name) values ('demo')"); err != nil {
				th.t.Errorf("Failed to execute DB query: %v", err)
			}
			if err := tx.Commit(); err != nil {
				th.t.Errorf("Failed committing transaction: %v", err)
			}
			th.t.Logf("testHandler END")
		}
		ctx := context.TODO()
		r, _ := http.NewRequestWithContext(ctx, http.MethodPost, "/", nil)
		rr := httptest.NewRecorder()
		t.Logf("Sending request 1...")
		database.TransactionHandler(&th).ServeHTTP(rr, r)
		t.Logf("Request 1 DONE.")
	}
	{
		var count int
		th.serve = func(rw http.ResponseWriter, r *http.Request) {
			th.t.Logf("testHandler BEGIN")
			tx := database.FromContext(r.Context())
			if _, err := tx.Query("select count(1) from category where name = 'demo'", func(r database.Result) error {
				return r.Scan(&count)
			}); err != nil {
				th.t.Errorf("Failed executing DB query: %v", err)
			}
			th.t.Logf("testHandler END")
		}
		ctx := context.TODO()
		r, _ := http.NewRequestWithContext(ctx, http.MethodPost, "/", nil)
		rr := httptest.NewRecorder()
		t.Logf("Sending request 2...")
		database.TransactionHandler(&th).ServeHTTP(rr, r)
		if count != 1 {
			t.Errorf("Expected 1 record, have %v", count)
		}
		t.Logf("Request 2 DONE.")
	}
	{
		var count int
		th.serve = func(rw http.ResponseWriter, r *http.Request) {
			th.t.Logf("testHandler BEGIN")
			tx := database.FromContext(r.Context())
			if _, err := tx.Query("1select count(1) from category where name = 'demo'", func(r database.Result) error {
				return r.Scan(&count)
			}); err == nil {
				th.t.Errorf("Expected ERROR, but got success")
			}
			th.t.Logf("testHandler END")
		}
		ctx := context.TODO()
		r, _ := http.NewRequestWithContext(ctx, http.MethodPost, "/", nil)
		rr := httptest.NewRecorder()
		t.Logf("Sending request 3...")
		database.TransactionHandler(&th).ServeHTTP(rr, r)
		if count != 0 {
			t.Errorf("Expected 0 record, have %v", count)
		}
		t.Logf("Request 3 DONE.")
	}
}
