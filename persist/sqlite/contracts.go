package sqlite

import (
	"time"

	"go.sia.tech/contract-revenue/stats"
)

func (s *Store) Metrics(timestamp time.Time) (stats stats.ContractState, err error) {
	err = s.transaction(func(tx txn) error {
		const query = `SELECT active_contracts, valid_contracts, missed_contracts, total_payouts, estimated_revenue
FROM hourly_contract_stats 
WHERE date_created <=$1 
ORDER BY date_created DESC 
LIMIT 1`
		stats.Timestamp = timestamp
		return tx.QueryRow(query, timestamp).Scan(&stats.Active, &stats.Valid, &stats.Missed, (*sqlCurrency)(&stats.Payout), (*sqlCurrency)(&stats.Revenue))
	})
	return
}

func (s *Store) Periods(start, end time.Time, period string) (state []stats.ContractState, err error) {
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
			if len(state) == 0 || state[len(state)-1].Timestamp == stat.Timestamp {
				state = append(state, stat)
			} else {
				state[len(state)-1] = stat
			}
		}
		return nil
	})
	return
}
