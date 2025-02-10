package database

import (
	"bytes"
	"context"
	"fmt"

	"github.com/jackc/pgtype"

	shopspring "github.com/jackc/pgtype/ext/shopspring-numeric"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

var pool *pgxpool.Pool

// ConfigureFromString връща нов Connection pool към базата
func ConfigureFromString(cfg string) {
	if pool == nil {
		config, err := pgxpool.ParseConfig(cfg)
		if err != nil {
			panic(err)
		}
		//config.ConnConfig.PreferSimpleProtocol = true
		config.ConnConfig.LogLevel = pgx.LogLevelTrace
		config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
			conn.ConnInfo().RegisterDataType(pgtype.DataType{
				Value: &shopspring.Numeric{},
				Name:  "numeric",
				OID:   pgtype.NumericOID,
			})
			return nil
		}
		p, err := pgxpool.ConnectConfig(context.Background(), config)
		if err != nil {
			panic(err)
		}
		pool = p
	}
}

// ConfigureFromDatabaseConfig връща нов Connection pool към базата
func ConfigureFromDatabaseConfig(config *DatabaseConfig) {
	ConfigureFromString(config.String())
}

// String се използва за конвертиране на DatabaseConfig към string
func (c DatabaseConfig) String() string {
	buffer := bytes.NewBufferString("")
	add := func(key, value string) {
		if value != "" {
			if buffer.Len() > 0 {
				buffer.WriteString(" ")
			}
			fmt.Fprintf(buffer, "%s=%s", key, value)
		}
	}
	add("host", c.Host)
	if c.Port != 0 {
		if buffer.Len() > 0 {
			buffer.WriteString(" ")
		}
		fmt.Fprintf(buffer, "port=%d", c.Port)
	}
	add("database", c.Database)
	add("user", c.User)
	add("password", c.Password)
	return buffer.String()
}
