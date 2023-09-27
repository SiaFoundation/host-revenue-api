package api

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
