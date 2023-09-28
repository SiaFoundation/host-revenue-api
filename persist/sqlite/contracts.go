package sqlite

import (
	"database/sql"
	"errors"
	"time"

	"go.sia.tech/contract-revenue/stats"
)

func (s *Store) Metrics(timestamp time.Time) (stats stats.ContractState, err error) {
	err = s.transaction(func(tx txn) error {
		const query = `SELECT active_contracts, valid_contracts, missed_contracts, total_payouts, estimated_revenue
FROM hourly_contract_stats 
WHERE date_created <= $1 
ORDER BY date_created DESC 
LIMIT 1`
		stats.Timestamp = timestamp
		return tx.QueryRow(query, timestamp).Scan(&stats.Active, &stats.Valid, &stats.Missed, (*sqlCurrency)(&stats.Payout), (*sqlCurrency)(&stats.Revenue))
	})
	return
}

func (s *Store) Periods(start, end time.Time, period string) (state []stats.ContractState, err error) {
	values := make(map[int64]stats.ContractState)
	err = s.transaction(func(tx txn) error {
		const query = `SELECT active_contracts, valid_contracts, missed_contracts, total_payouts, estimated_revenue, date_created
FROM hourly_contract_stats
WHERE date_created BETWEEN $1 AND $2
ORDER BY date_created ASC`

		rows, err := tx.Query(query, start, end)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var stat stats.ContractState
			if err := rows.Scan(&stat.Active, &stat.Valid, &stat.Missed, (*sqlCurrency)(&stat.Payout), (*sqlCurrency)(&stat.Revenue), (*sqlTime)(&stat.Timestamp)); err != nil {
				return err
			}

			stat.Timestamp = stats.NormalizePeriod(stat.Timestamp, period)
			values[stat.Timestamp.Unix()] = stat
		}

		stmt, err := tx.Prepare(`SELECT usd_rate, eur_rate, btc_rate FROM market_data ORDER BY ABS(date_created - $1) LIMIT 1`)
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, stat := range values {
			err := stmt.QueryRow(sqlTime(stat.Timestamp)).Scan(&stat.ExchangeRates.USD, &stat.ExchangeRates.EUR, &stat.ExchangeRates.BTC)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
		}

		return nil
	})

	// build the empty array
	for t := start; t.Before(end); t = nextPeriod(t, period) {
		state = append(state, stats.ContractState{Timestamp: t})
	}

	// fill in the values from the database
	prev := state[0]
	for i := range state {
		timestamp := state[i].Timestamp
		v, ok := values[timestamp.Unix()]
		if !ok {
			v = prev
		}
		v.Timestamp = timestamp
		state[i], prev = v, v
	}
	return
}

func nextPeriod(timestamp time.Time, period string) time.Time {
	switch period {
	case stats.PeriodHourly:
		return timestamp.Add(time.Hour)
	case stats.PeriodDaily:
		return timestamp.AddDate(0, 0, 1)
	case stats.PeriodWeekly:
		return timestamp.AddDate(0, 0, 7)
	case stats.PeriodMonthly:
		return timestamp.AddDate(0, 1, 0)
	default:
		panic("invalid period")
	}
}
