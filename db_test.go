package database_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	db "github.com/mmitevski/database"
)

func TestDatabaseConfigToString(t *testing.T) {
	config := &db.DatabaseConfig{
		Host:     "host1",
		Port:     54322,
		User:     "test1",
		Password: "test2",
		Database: "test3",
	}
	expected := "host=host1 port=54322 database=test3 user=test1 password=test2"
	got := config.String()
	if got != expected {
		t.Fatalf("Expected \"%s\", got \"%s\"", expected, got)
	}
}

func connectAndCheck(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tx := db.FromContext(r.Context())
		if tx == nil {
			t.Fatal("error obtaining transaction from context")
		}
		if count, err := tx.Query("select 1", func(r db.Result) error {
			value := 0
			err := r.Scan(&value)
			if err != nil {
				return err
			}
			if value != 1 {
				return fmt.Errorf("Expected 1, got %d", value)
			}
			return nil
		}); count != 1 || err != nil {
			if err != nil {
				t.Fatal(err)
			}
			if count != 1 {
				t.Fatalf("Expected 1, got %d", count)
			}
		}
		if count, err := tx.Query("select 1", func(r db.Result) error {
			values, err := r.Values()
			if err != nil {
				return err
			}
			value, ok := values[0].(int32)
			if ok {
				if value != 1 {
					return fmt.Errorf("Expected 1, got %d", value)
				}
			} else {
				return fmt.Errorf("Error converting returned value %s to %T", values[0], value)
			}
			return nil
		}); count != 1 || err != nil {
			if err != nil {
				t.Fatal(err)
			}
			if count != 1 {
				t.Fatalf("Expected 1, got %d", count)
			}
		}
	})
	request := httptest.NewRequest("GET", "/", nil)
	recorder := httptest.NewRecorder()
	db.TransactionHandler(handler).ServeHTTP(recorder, request)
}

func TestConfigureFromString(t *testing.T) {
	db.ConfigureFromString("host=localhost database=test user=test password=test")
	connectAndCheck(t)
}

func TestConfigureFromConfigureFromDatabaseConfig(t *testing.T) {
	config := db.DatabaseConfig{
		Host:     "localhost",
		Port:     5432,
		User:     "test",
		Password: "test",
		Database: "test",
	}
	db.ConfigureFromDatabaseConfig(&config)
	connectAndCheck(t)
}

func executeOrFail(t *testing.T, tx db.Transaction, sql string, args ...interface{}) {
	if _, err := tx.Execute(sql, args...); err != nil {
		t.Fatal(err)
	}
}

func createTestTable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Log("Creating test table...")
	tx := db.NewTransaction(ctx)
	executeOrFail(t, tx, "drop table if exists test")
	executeOrFail(t, tx, "create table test(id integer)")
	tx.Commit()
}

func insertTestRecords(t *testing.T, args ...interface{}) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Log("Inserting test records...")
	tx := db.NewTransaction(ctx)
	for _, arg := range args {
		executeOrFail(t, tx, "insert into test(id) values ($1)", arg)
	}
	tx.Commit()
}

func TestCreateTableCRUDandDropTable(t *testing.T) {
	t.Log("Connecting to database...")
	db.ConfigureFromString("host=localhost database=test user=test password=test")
	createTestTable(t)
	insertTestRecords(t, 1, 2, 3)
	tx := db.NewTransaction(context.Background())
	ids := ""
	_, err := tx.Query("select id from test order by id", func(r db.Result) error {
		var i int
		err := r.Scan(&i)
		ids += fmt.Sprint(i)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Checking returned datas validity...")
	if ids != "123" {
		t.Fatal("Expected data to be '123', but '" + ids + "' found.")
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	//tx.Execute("drop table test")
}
