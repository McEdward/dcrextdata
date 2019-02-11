package main

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
)

type PgDb struct {
	*sql.DB
}

var (
	insertExchangeDataStmt      = `INSERT INTO exchange_data (high, low, open, close, time, exchange) VALUES ($1, $2, $3, $4, $5, $6)`
	createExchangeDataStmt      = `CREATE TABLE IF NOT EXISTS exchange_data (high FLOAT8, low FLOAT8, open FLOAT8, close FLOAT8, time INT, exchange VARCHAR(25), CONSTRAINT tick PRIMARY KEY (time, exchange))`
	getLastExchangeDataTimeStmt = `SELECT time FROM exchange_data ORDER BY time DESC LIMIT 1`
	insertPOWDataStmt           = `INSERT INTO pow_stats (time, network_hashrate, pool_hashrate, workers, network_difficulty, coin_price, btc_price, source) VALUES ($1, $2 ,$3 ,$4 ,$5 ,$6 ,$7, $8)`
	createPOWDataStmt           = `CREATE TABLE IF NOT EXISTS pow_stats (time INT, network_hashrate INT, pool_hashrate FLOAT, workers INT, network_difficulty FLOAT8, coin_price VARCHAR(25), btc_price VARCHAR(25), source VARCHAR(25), PRIMARY KEY (time, source))`
	getLastPOWDataTimeStmt      = `SELECT time FROM pow_stats ORDER BY time DESC LIMIT 1`
)

func NewPgDb(psqlInfo string) (PgDb, error) {
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return PgDb{nil}, err
	}
	return PgDb{db}, nil
}

func (db *PgDb) CreateExchangeDataTable() error {
	_, err := db.Exec(createExchangeDataStmt)
	return err
}

func (db *PgDb) tableExists(name string) (bool, error) {
	rows, err := db.Query(`SELECT relname FROM pg_class WHERE relname = $1`, name)
	if err == nil {
		defer func() {
			if e := rows.Close(); e != nil {
				log.Error("Close of Query failed: ", e)
			}
		}()
		return rows.Next(), nil
	}
	return false, err
}

func (db *PgDb) ExchangeDataTableExits() bool {
	exists, _ := db.tableExists("exchange_data")
	return exists
}

func (db *PgDb) AddExchangeData(data []exchangeDataTick) error {
	added := 0
	for _, v := range data {
		_, err := db.Exec(insertExchangeDataStmt, v.High, v.Low, v.Open, v.Close, v.Time, v.Exchange)
		if err != nil {
			if !strings.Contains(err.Error(), "unique constraint") { // Ignore duplicate entries
				return err
			}
			added++
		}
	}
	log.Debug("Succesfully added entries: ", added)
	return nil
}

func (db *PgDb) LastExchangeEntryTime() (int64, error) {
	var time int64 = -1
	rows := db.QueryRow(getLastExchangeDataTimeStmt)
	err := rows.Scan(&time)

	if err != nil {
		return time, err
	}
	return time, nil
}

func (db *PgDb) DropTable(name string) error {
	_, err := db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s;`, name))
	return err
}

func (db *PgDb) DropExchangeDataTable() error {
	return db.DropTable("exchange_data")
}

func (db *PgDb) CreatePOWDataTable() error {
	_, err := db.Exec(createPOWDataStmt)
	return err
}

func (db *PgDb) AddPOWData(data []powDataTick) error {
	added := 0
	for _, j := range data {
		_, err := db.Exec(insertPOWDataStmt, j.Time, j.NetworkHashrate, j.PoolHashrate, j.Workers, j.NetworkDifficulty, j.CoinPrice, j.BtcPrice, j.source)
		if err != nil {
			if !strings.Contains(err.Error(), "unique constraint") { // Ignore duplicate entries
				return err
			}
			added++
		}
	}
	log.Debug("Succesfully added pow entries: ", added)
	return nil
}

func (db *PgDb) LastPOWDataTime() (int64, error) {
	var time int64 = -1
	rows := db.QueryRow(getLastPOWDataTimeStmt)
	err := rows.Scan(&time)

	if err != nil {
		return time, err
	}
	return time, nil
}
func (db *PgDb) POWTableExits() bool {
	exists, _ := db.tableExists("pow_stats")
	return exists
}
