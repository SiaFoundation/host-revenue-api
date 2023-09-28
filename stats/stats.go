package stats

import (
	"time"

	"go.sia.tech/core/types"
	"go.uber.org/zap"
)

const (
	PeriodDaily   = "daily"
	PeriodHourly  = "hourly"
	PeriodWeekly  = "weekly"
	PeriodMonthly = "monthly"
)

type (
	Contract struct {
		ID                   types.FileContractID
		BlockID              types.BlockID
		Height               uint64
		InitialValid         types.Currency
		InitialMissed        types.Currency
		FinalValid           types.Currency
		FinalMissed          types.Currency
		InitialValidRevenue  types.Currency
		InitialMissedRevenue types.Currency
		ProofHeight          uint64
		ExpirationHeight     uint64
	}

	ExchangeRates struct {
		USD float64 `json:"usd"`
		EUR float64 `json:"eur"`
		BTC float64 `json:"btc"`
	}

	ContractState struct {
		Active        int            `json:"active"`
		Valid         int            `json:"valid"`
		Missed        int            `json:"missed"`
		Revenue       types.Currency `json:"revenue"`
		Payout        types.Currency `json:"payout"`
		ExchangeRates ExchangeRates  `json:"exchangeRates"`
		Timestamp     time.Time      `json:"timestamp"`
	}

	Store interface {
		Metrics(time.Time) (ContractState, error)
		Periods(start, end time.Time, period string) ([]ContractState, error)
	}

	// A Provider indexes stats on the current state of the Sia network.
	Provider struct {
		log *zap.Logger

		store Store
	}
)

func (p *Provider) Metrics(timestamp time.Time) (ContractState, error) {
	return p.store.Metrics(timestamp)
}

func (p *Provider) Periods(start, end time.Time, periods string) ([]ContractState, error) {
	return p.store.Periods(start, end, periods)
}

// NewProvider creates a new Provider.
func NewProvider(s Store, log *zap.Logger) (*Provider, error) {
	p := &Provider{
		log:   log,
		store: s,
	}
	return p, nil
}

func NormalizePeriod(timestamp time.Time, period string) time.Time {
	switch period {
	case PeriodHourly:
		return timestamp.Truncate(time.Hour)
	case PeriodDaily:
		y, m, d := timestamp.Date()
		return time.Date(y, m, d, 0, 0, 0, 0, timestamp.Location())
	case PeriodWeekly:
		y, m, d := timestamp.Date()
		return time.Date(y, m, d, 0, 0, 0, 0, timestamp.Location()).AddDate(0, 0, -int(timestamp.Weekday()))
	case PeriodMonthly:
		y, m, _ := timestamp.Date()
		return time.Date(y, m, 1, 0, 0, 0, 0, timestamp.Location())
	}
	return timestamp
}
