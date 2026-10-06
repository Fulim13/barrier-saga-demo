package dao

import (
	"database/sql"
	"errors"
	"log"
	"os"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const defaultDSN = "postgres://postgres:postgres@127.0.0.1:5432/barrier?sslmode=disable"

func dsn() string {
	if url := os.Getenv("DATABASE_URL"); url != "" {
		return url
	}
	return defaultDSN
}

var (
	dbOnce sync.Once
	db     *sql.DB
)

func GetDB() *sql.DB {
	dbOnce.Do(func() {
		var err error
		db, err = sql.Open("pgx", dsn())
		if err != nil {
			log.Panicf("cannot open postgres: %s", err)
		}
		// Branches block on each other inside the barrier, so each in-flight
		// branch holds a connection. Keep the pool comfortably larger than the
		// number of concurrent branches.
		db.SetMaxOpenConns(20)
		db.SetMaxIdleConns(10)
		db.SetConnMaxLifetime(time.Hour)
		if err = db.Ping(); err != nil {
			log.Panicf("cannot reach postgres at %s: %s", dsn(), err)
		}
	})
	return db
}

func AdjustBalance(uid int, delta int) error {
	db := GetDB()
	result, err := db.Exec(
		"update bank_account set balance = balance + $1 where user_id = $2 and balance + $1 >= 0",
		delta, uid)
	if err != nil {
		log.Printf("failed to adjust the account balance: %s", err)
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return errors.New("the balance would go negative, or the uid does not exist")
	}
	return nil
}
