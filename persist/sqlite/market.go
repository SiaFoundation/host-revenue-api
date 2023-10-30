package sqlite

import (
	"database/sql"
	"errors"
	"time"

	"github.com/shopspring/decimal"
)

var (
	ErrNoData = errors.New("no data")
)

// AddMarketData adds a new market data point to the database.
func (s *Store) AddMarketData(usd, eur, btc decimal.Decimal, timestamp time.Time) error {
	return s.transaction(func(tx txn) error {
		const query = `INSERT INTO market_data (usd_rate, eur_rate, btc_rate, date_created)
VALUES ($1, $2, $3, $4)
ON CONFLICT (date_created) DO UPDATE SET usd_rate=EXCLUDED.usd_rate, eur_rate=EXCLUDED.eur_rate, btc_rate=EXCLUDED.btc_rate`
		_, err := tx.Exec(query, usd, eur, btc, sqlTime(timestamp))
		return err
	})
}

// GetExchangeRate returns the most recent exchange rate.
func (s *Store) GetExchangeRate() (usd, eur, btc decimal.Decimal, timestamp time.Time, err error) {
	err = s.transaction(func(tx txn) error {
		const query = `SELECT usd_rate, eur_rate, btc_rate, date_created FROM market_data ORDER BY date_created DESC LIMIT 1`
		return tx.QueryRow(query).Scan(&usd, &eur, &btc, (*sqlTime)(&timestamp))
	})
	if errors.Is(err, sql.ErrNoRows) {
		err = ErrNoData
	}
	return
}
