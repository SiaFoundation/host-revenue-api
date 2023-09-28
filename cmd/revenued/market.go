package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/siacentral/apisdkgo/sia"
	"go.sia.tech/host-revenue-api/build"
	"go.sia.tech/host-revenue-api/persist/sqlite"
	"go.uber.org/zap"
)

func updateMarketData(store *sqlite.Store, timestamp time.Time) error {
	scc := sia.NewClient()
	rates, err := scc.GetHistoricalExchangeRate(timestamp)
	if err != nil {
		return fmt.Errorf("failed to fetch exchange rate: %w", err)
	}

	if err := store.AddMarketData(rates["usd"], rates["eur"], rates["btc"], timestamp); err != nil {
		return fmt.Errorf("failed to add market data: %w", err)
	}
	return nil
}

func syncMarketData(ctx context.Context, store *sqlite.Store, log *zap.Logger) {
	_, _, _, timestamp, err := store.GetExchangeRate()
	if err != nil && !errors.Is(err, sqlite.ErrNoData) {
		log.Error("failed to get exchange rate", zap.Error(err))
		return
	}

	_, genesis := build.Network()
	if timestamp.Before(genesis.Timestamp) {
		timestamp = genesis.Timestamp
	}

	if time.Since(timestamp) > 15*time.Minute {
		// catch up on missing data
		end := time.Now().Truncate(time.Hour)
		for current := timestamp.Truncate(time.Hour); current.Before(end); {
			select {
			case <-ctx.Done():
				return
			default:
			}

			if err := updateMarketData(store, current); err != nil {
				log.Warn("failed to update market data", zap.Error(err), zap.Time("timestamp", current))
			} else {
				current = current.Add(time.Hour) // only increment on success, retry on failure
			}
			time.Sleep(time.Second) // rate limit
		}
	}

	t := time.NewTicker(5 * time.Minute)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}

		if err := updateMarketData(store, time.Now().Truncate(time.Hour)); err != nil {
			log.Warn("failed to update market data", zap.Error(err))
		}
	}

}
