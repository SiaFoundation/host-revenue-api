package stats

import (
	"time"

	"github.com/shopspring/decimal"
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

	Values struct {
		SC  types.Currency  `json:"sc"`
		USD decimal.Decimal `json:"usd"`
		EUR decimal.Decimal `json:"eur"`
		BTC decimal.Decimal `json:"btc"`
	}

	ContractState struct {
		Active    int       `json:"active"`
		Valid     int       `json:"valid"`
		Missed    int       `json:"missed"`
		Revenue   Values    `json:"revenue"`
		Payout    Values    `json:"payout"`
		Timestamp time.Time `json:"timestamp"`
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

func (v Values) Add(b Values) Values {
	return Values{
		SC:  v.SC.Add(b.SC),
		USD: v.USD.Add(b.USD),
		EUR: v.EUR.Add(b.EUR),
		BTC: v.BTC.Add(b.BTC),
	}
}

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
