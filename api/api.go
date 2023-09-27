package api

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"go.sia.tech/contract-revenue/stats"
	"go.sia.tech/jape"
	"go.uber.org/zap"
)

type (

	// A StatProvider provides statistics about the current state of the Sia network.
	StatProvider interface {
		Metrics(timestamp time.Time) (stats.ContractState, error)
		Periods(start, end time.Time, period string) ([]stats.ContractState, error)
	}

	api struct {
		log *zap.Logger

		sp StatProvider
	}
)

func (a *api) handleGetRevenue(c jape.Context) {
	var timestamp time.Time
	if err := c.DecodeForm("timestamp", &timestamp); err != nil {
		return
	}

	if timestamp.IsZero() {
		timestamp = time.Now()
	}

	state, err := a.sp.Metrics(timestamp)
	if err != nil {
		c.Error(err, http.StatusInternalServerError)
		return
	}
	c.Encode(state)
}

func (a *api) handleGetRevenuePeriods(c jape.Context) {
	var period string
	if err := c.DecodeParam("period", &period); err != nil {
		return
	}

	var start, end time.Time
	if err := c.DecodeForm("start", &start); err != nil {
		return
	} else if err := c.DecodeForm("end", &end); err != nil {
		return
	}

	if start.IsZero() || end.IsZero() {
		c.Error(errors.New("start and end are required"), http.StatusBadRequest)
		return
	} else if end.Before(start) {
		c.Error(errors.New("end must be after start"), http.StatusBadRequest)
	}

	switch period {
	case stats.PeriodHourly:
		start = start.Truncate(time.Hour)
		end = end.Add(time.Hour).Truncate(time.Hour)
	case stats.PeriodDaily:
		y, m, d := start.Date()
		start = time.Date(y, m, d, 0, 0, 0, 0, start.Location())
		y, m, d = end.Date()
		end = time.Date(y, m, d, 0, 0, 0, 0, end.Location()).AddDate(0, 0, 1)
	case stats.PeriodWeekly:
		y, m, d := start.Date()
		start = time.Date(y, m, d, 0, 0, 0, 0, start.Location()).AddDate(0, 0, -int(start.Weekday()))
		y, m, d = end.Date()
		end = time.Date(y, m, d, 0, 0, 0, 0, end.Location()).AddDate(0, 0, 7-int(end.Weekday()))
	case stats.PeriodMonthly:
		y, m, _ := start.Date()
		start = time.Date(y, m, 1, 0, 0, 0, 0, start.Location())
		y, m, _ = end.Date()
		end = time.Date(y, m+1, 1, 0, 0, 0, 0, end.Location())
	default:
		c.Error(fmt.Errorf("invalid period %q", period), http.StatusBadRequest)
		return
	}

	revenue, err := a.sp.Periods(start, end, period)
	if err != nil {
		c.Error(err, http.StatusInternalServerError)
		return
	}
	c.Encode(revenue)
}

func (a *api) handleGetWeb3Index(c jape.Context) {
	var resp Web3IndexResp

	now := time.Now()
	oneDayAgo := now.AddDate(0, 0, -1)
	twoDaysAgo := now.AddDate(0, 0, -2)
	oneWeekAgo := now.AddDate(0, 0, -7)
	twoWeeksAgo := now.AddDate(0, 0, -14)
	thirtyDaysAgo := now.AddDate(0, 0, -30)
	sixtyDaysAgo := now.AddDate(0, 0, -60)
	ninetyDaysAgo := now.AddDate(0, 0, -90)

	// TODO: exchange rate
	revenue, err := a.sp.Metrics(now)
	if err != nil {
		c.Error(err, http.StatusInternalServerError)
		return
	}
	resp.Revenue.Now, _ = revenue.Revenue.Big().Float64()

	revenue, err = a.sp.Metrics(oneDayAgo)
	if err != nil {
		c.Error(err, http.StatusInternalServerError)
		return
	}
	resp.Revenue.OneDayAgo, _ = revenue.Revenue.Big().Float64()

	revenue, err = a.sp.Metrics(twoDaysAgo)
	if err != nil {
		c.Error(err, http.StatusInternalServerError)
		return
	}
	resp.Revenue.TwoDaysAgo, _ = revenue.Revenue.Big().Float64()

	revenue, err = a.sp.Metrics(oneWeekAgo)
	if err != nil {
		c.Error(err, http.StatusInternalServerError)
		return
	}
	resp.Revenue.OneWeekAgo, _ = revenue.Revenue.Big().Float64()

	revenue, err = a.sp.Metrics(twoWeeksAgo)
	if err != nil {
		c.Error(err, http.StatusInternalServerError)
		return
	}
	resp.Revenue.TwoWeeksAgo, _ = revenue.Revenue.Big().Float64()

	revenue, err = a.sp.Metrics(thirtyDaysAgo)
	if err != nil {
		c.Error(err, http.StatusInternalServerError)
		return
	}
	resp.Revenue.ThirtyDaysAgo, _ = revenue.Revenue.Big().Float64()

	revenue, err = a.sp.Metrics(sixtyDaysAgo)
	if err != nil {
		c.Error(err, http.StatusInternalServerError)
		return
	}
	resp.Revenue.SixtyDaysAgo, _ = revenue.Revenue.Big().Float64()

	revenue, err = a.sp.Metrics(ninetyDaysAgo)
	if err != nil {
		c.Error(err, http.StatusInternalServerError)
		return
	}
	resp.Revenue.NinetyDaysAgo, _ = revenue.Revenue.Big().Float64()

	y, m, _ := now.Date()
	start := time.Date(y-2, m, 1, 0, 0, 0, 0, now.Location())

	days, err := a.sp.Periods(start, now, stats.PeriodDaily)
	if err != nil {
		c.Error(err, http.StatusInternalServerError)
		return
	}

	for i := 0; i < len(days); i++ {
		rev, _ := days[i].Revenue.Big().Float64()
		resp.Days = append(resp.Days, Web3IndexDay{
			Date:    days[i].Timestamp.Unix(),
			Revenue: rev,
		})
	}
	c.Encode(resp)
}

// NewServer returns an http.Handler that serves the API.
func NewServer(sp StatProvider, log *zap.Logger) http.Handler {
	a := &api{
		log: log,
		sp:  sp,
	}

	return jape.Mux(map[string]jape.Handler{
		"GET /revenue":           a.handleGetRevenue,
		"GET /revenue/:period":   a.handleGetRevenuePeriods,
		"GET /web3index/revenue": a.handleGetWeb3Index,
	})
}
