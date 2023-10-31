package api

import (
	"net/http"
	"time"

	"go.sia.tech/host-revenue-api/stats"
	"go.sia.tech/jape"
)

type (
	Web3IndexDay struct {
		Date    int64   `json:"date"`
		Revenue float64 `json:"revenue"`
	}

	Web3IndexRevenue struct {
		Now           float64 `json:"now"`
		OneDayAgo     float64 `json:"oneDayAgo"`
		TwoDaysAgo    float64 `json:"twoDaysAgo"`
		OneWeekAgo    float64 `json:"oneWeekAgo"`
		TwoWeeksAgo   float64 `json:"twoWeeksAgo"`
		ThirtyDaysAgo float64 `json:"thirtyDaysAgo"`
		SixtyDaysAgo  float64 `json:"sixtyDaysAgo"`
		NinetyDaysAgo float64 `json:"ninetyDaysAgo"`
	}

	Web3IndexResp struct {
		Days    []Web3IndexDay   `json:"days"`
		Revenue Web3IndexRevenue `json:"revenue"`
	}
)

func (a *api) handleGetWeb3Index(c jape.Context) {
	var resp Web3IndexResp

	now := time.Now()
	revenue, err := a.sp.Metrics(now)
	if err != nil {
		c.Error(err, http.StatusInternalServerError)
		return
	}
	resp.Revenue.Now = revenue.Revenue.USD.InexactFloat64()

	oneDayAgo := now.AddDate(0, 0, -1)
	revenue, err = a.sp.Metrics(oneDayAgo)
	if err != nil {
		c.Error(err, http.StatusInternalServerError)
		return
	}
	resp.Revenue.OneDayAgo = revenue.Revenue.USD.InexactFloat64()

	twoDaysAgo := now.AddDate(0, 0, -2)
	revenue, err = a.sp.Metrics(twoDaysAgo)
	if err != nil {
		c.Error(err, http.StatusInternalServerError)
		return
	}
	resp.Revenue.TwoDaysAgo = revenue.Revenue.USD.InexactFloat64()

	oneWeekAgo := now.AddDate(0, 0, -7)
	revenue, err = a.sp.Metrics(oneWeekAgo)
	if err != nil {
		c.Error(err, http.StatusInternalServerError)
		return
	}
	resp.Revenue.OneWeekAgo = revenue.Revenue.USD.InexactFloat64()

	twoWeeksAgo := now.AddDate(0, 0, -14)
	revenue, err = a.sp.Metrics(twoWeeksAgo)
	if err != nil {
		c.Error(err, http.StatusInternalServerError)
		return
	}
	resp.Revenue.TwoWeeksAgo = revenue.Revenue.USD.InexactFloat64()

	thirtyDaysAgo := now.AddDate(0, 0, -30)
	revenue, err = a.sp.Metrics(thirtyDaysAgo)
	if err != nil {
		c.Error(err, http.StatusInternalServerError)
		return
	}
	resp.Revenue.ThirtyDaysAgo = revenue.Revenue.USD.InexactFloat64()

	sixtyDaysAgo := now.AddDate(0, 0, -60)
	revenue, err = a.sp.Metrics(sixtyDaysAgo)
	if err != nil {
		c.Error(err, http.StatusInternalServerError)
		return
	}
	resp.Revenue.SixtyDaysAgo = revenue.Revenue.USD.InexactFloat64()

	ninetyDaysAgo := now.AddDate(0, 0, -90)
	revenue, err = a.sp.Metrics(ninetyDaysAgo)
	if err != nil {
		c.Error(err, http.StatusInternalServerError)
		return
	}
	resp.Revenue.NinetyDaysAgo = revenue.Revenue.USD.InexactFloat64()

	start := now.AddDate(-1, 0, 0)
	start = start.AddDate(0, 0, -int(start.Weekday()+1))
	days, err := a.sp.Periods(start, now, stats.PeriodDaily)
	if err != nil {
		c.Error(err, http.StatusInternalServerError)
		return
	}

	for i := len(days) - 1; i > 0; i-- {
		current, prev := days[i], days[i-1]
		resp.Days = append(resp.Days, Web3IndexDay{
			Date:    current.Timestamp.Unix(),
			Revenue: current.Revenue.USD.Sub(prev.Revenue.USD).InexactFloat64(),
		})
	}
	c.Encode(resp)
}
