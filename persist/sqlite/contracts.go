package sqlite

import (
	"fmt"
	"time"

	"go.sia.tech/host-revenue-api/stats"
)

func scanContractState(row scanner) (state stats.ContractState, err error) {
	err = row.Scan(&state.Active, &state.Valid, &state.Missed, (*sqlCurrency)(&state.Payout.SC), &state.Payout.USD, &state.Payout.EUR, &state.Payout.BTC, (*sqlCurrency)(&state.Revenue.SC), &state.Revenue.USD, &state.Revenue.EUR, &state.Revenue.BTC, (*sqlTime)(&state.Timestamp))
	return
}

func getMetrics(tx txn, timestamp time.Time) (stats.ContractState, error) {
	const query = `SELECT active_contracts, valid_contracts, missed_contracts, 
total_payouts_sc, total_payouts_usd, total_payouts_eur, total_payouts_btc,
estimated_revenue_sc, estimated_revenue_usd, estimated_revenue_eur, estimated_revenue_btc,
date_created 
FROM hourly_contract_stats 
WHERE date_created <= $1 
ORDER BY date_created DESC 
LIMIT 1`

	row := tx.QueryRow(query, sqlTime(timestamp))
	state, err := scanContractState(row)
	return state, err
}

func (s *Store) Metrics(timestamp time.Time) (state stats.ContractState, err error) {
	err = s.transaction(func(tx txn) error {
		state, err = getMetrics(tx, timestamp)
		return err
	})
	return
}

func (s *Store) Periods(start, end time.Time, period string) (state []stats.ContractState, err error) {
	values := make(map[int64]stats.ContractState)
	err = s.transaction(func(tx txn) error {
		const query = `SELECT active_contracts, valid_contracts, missed_contracts, 
total_payouts_sc, total_payouts_usd, total_payouts_eur, total_payouts_btc,
estimated_revenue_sc, estimated_revenue_usd, estimated_revenue_eur, estimated_revenue_btc,
date_created
FROM hourly_contract_stats
WHERE date_created BETWEEN $1 AND $2
ORDER BY date_created ASC`
		start = stats.NormalizePeriod(start, period)
		end = stats.NormalizePeriod(end, period)

		rows, err := tx.Query(query, sqlTime(start), sqlTime(end))
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			state, err := scanContractState(rows)
			if err != nil {
				return fmt.Errorf("failed to scan contract state: %w", err)
			}

			state.Timestamp = stats.NormalizePeriod(state.Timestamp.In(start.Location()), period)
			values[state.Timestamp.Unix()] = state
		}
		return nil
	})

	// build the array
	var prev stats.ContractState
	for t := start; t.Before(end); t = nextPeriod(t, period) {
		v, ok := values[t.Unix()]
		if !ok {
			v = prev
		}
		v.Timestamp = t
		state = append(state, v)
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
