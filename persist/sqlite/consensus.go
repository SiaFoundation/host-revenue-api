package sqlite

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/shopspring/decimal"
	"gitlab.com/NebulousLabs/encoding"
	"go.sia.tech/core/types"
	"go.sia.tech/host-revenue-api/stats"
	"go.sia.tech/siad/modules"
	stypes "go.sia.tech/siad/types"
	"go.uber.org/zap"
)

var (
	maturityDelay = uint64(stypes.MaturityDelay)
)

// LastChange returns the last consensus change processed by the store.
func (s *Store) LastChange() (ccID modules.ConsensusChangeID, err error) {
	value := nullable((*sqlHash256)(&ccID))
	err = s.db.QueryRow(`SELECT contracts_last_processed_change FROM global_settings LIMIT 1`).Scan(value)
	if errors.Is(err, sql.ErrNoRows) {
		return modules.ConsensusChangeBeginning, nil
	} else if err == nil && !value.Valid {
		return modules.ConsensusChangeBeginning, nil
	}
	return
}

func getExchangeRate(tx txn, timestamp time.Time) (usd, eur, btc decimal.Decimal, err error) {
	err = tx.QueryRow(`SELECT usd_rate, eur_rate, btc_rate FROM market_data ORDER BY ABS(date_created - $1) LIMIT 1`, sqlTime(timestamp)).Scan(
		&usd, &eur, &btc)
	if errors.Is(err, sql.ErrNoRows) {
		return decimal.Zero, decimal.Zero, decimal.Zero, errors.New("no exchange rate data")
	} else if err != nil {
		return decimal.Zero, decimal.Zero, decimal.Zero, err
	}
	return
}

// ProcessConsensusChange implements modules.ConsensusSetSubscriber.
func (s *Store) ProcessConsensusChange(cc modules.ConsensusChange) {
	log := s.log.Named("consensusChange").With(zap.Uint64("height", uint64(cc.BlockHeight)), zap.Stringer("changeID", cc.ID))

	err := s.transaction(func(tx txn) error {
		for _, reverted := range cc.RevertedBlocks {
			// note: since the stats are incremented only afer the payout matures,
			// there's no need to revert them when a block is reverted. The
			// payout value should still be the same.
			blockID := types.BlockID(reverted.ID())
			if err := revertBlock(tx, blockID); err != nil {
				return fmt.Errorf("failed to revert block %q: %w", blockID, err)
			}
			log.Debug("reverted block", zap.Stringer("blockID", blockID))
		}

		spentUtxoValues := make(map[types.SiacoinOutputID]types.Currency)
		for _, diff := range cc.AppliedDiffs {
			for _, scod := range diff.SiacoinOutputDiffs {
				if scod.Direction != modules.DiffRevert {
					continue
				}
				var spent types.Currency
				convertToCore(scod.SiacoinOutput.Value, &spent)
				spentUtxoValues[types.SiacoinOutputID(scod.ID)] = spent
			}
		}

		height := uint64(cc.BlockHeight) - uint64(len(cc.AppliedBlocks)) + 1
		for _, applied := range cc.AppliedBlocks {
			blockID := types.BlockID(applied.ID())
			timestamp := time.Unix(int64(applied.Timestamp), 0)
			log.Debug("adding block", zap.Stringer("blockID", blockID), zap.Time("timestamp", timestamp), zap.Uint64("height", height))
			blockDBID, err := addBlock(tx, blockID, height, timestamp)
			if err != nil {
				return fmt.Errorf("failed to add block %q: %w", applied.ID(), err)
			}

			var active int
			for _, txn := range applied.Transactions {
				var inputs []types.Currency
				for _, input := range txn.SiacoinInputs {
					value, ok := spentUtxoValues[types.SiacoinOutputID(input.ParentID)]
					if !ok {
						log.Panic("missing spent utxo value", zap.Stringer("utxoID", input.ParentID))
					}
					inputs = append(inputs, value)
				}

				var outputs []types.Currency
				for _, output := range txn.SiacoinOutputs {
					var value types.Currency
					convertToCore(output.Value, &value)
					outputs = append(outputs, value)
				}

				var fees types.Currency
				for _, fee := range txn.MinerFees {
					var value types.Currency
					convertToCore(fee, &value)
					fees = fees.Add(value)
				}

				for i, fc := range txn.FileContracts {
					fcID := types.FileContractID(txn.FileContractID(uint64(i)))

					var contract types.FileContract
					convertToCore(fc, &contract)

					// attempt to calculate the initial revenue for renewals.
					// This isn't guaranteed to be correct, but it's better than
					// nothing.
					var initialValidRevenue, initialMissedRevenue types.Currency
					if len(contract.ValidProofOutputs) >= 2 && len(contract.MissedProofOutputs) >= 2 && len(txn.FileContracts) == 1 { // ignore weird transactions with multiple contracts
						renterTarget := contract.ValidProofOutputs[0].Value.Add(fees)
						hostTarget := contract.MissedProofOutputs[1].Value

						hostFunds, ok := estimateHostFunds(inputs, outputs, renterTarget, hostTarget)
						if ok {
							v, underflow := contract.ValidHostPayout().SubWithUnderflow(hostFunds)
							if !underflow {
								initialValidRevenue = v
							}

							v, underflow = contract.MissedHostPayout().SubWithUnderflow(hostFunds)
							if !underflow {
								initialMissedRevenue = v
							}
						}
					}

					if err := addActiveContract(tx, fcID, contract, blockDBID, initialValidRevenue, initialMissedRevenue); err != nil {
						return fmt.Errorf("failed to add active contract %q: %w", fcID, err)
					}
					log.Debug("added active contract", zap.Stringer("contractID", fcID), zap.Uint64("expirationHeight", contract.WindowEnd))
					active++
				}

				for _, fcr := range txn.FileContractRevisions {
					fcID := types.FileContractID(fcr.ParentID)

					var validPayout, missedPayout types.Currency
					if len(fcr.NewValidProofOutputs) >= 2 {
						convertToCore(fcr.NewValidProofOutputs[1].Value, &validPayout)
					}
					if len(fcr.NewMissedProofOutputs) >= 2 {
						convertToCore(fcr.NewMissedProofOutputs[1].Value, &missedPayout)
					}

					if err := reviseContract(tx, fcID, validPayout, missedPayout); err != nil {
						return fmt.Errorf("failed to revise contract %q: %w", fcID, err)
					}
					log.Debug("revised contract", zap.Stringer("contractID", fcID))
				}

				for _, sco := range txn.StorageProofs {
					if err := proveContract(tx, types.FileContractID(sco.ParentID), blockDBID); err != nil {
						return fmt.Errorf("failed to prove contract %q: %w", sco.ParentID, err)
					}
					log.Debug("proved contract", zap.Stringer("contractID", sco.ParentID))
				}
			}

			var valid, missed int
			var totalRevenue, totalPayout stats.Values
			if height > maturityDelay {
				usdRate, eurRate, btcRate, err := getExchangeRate(tx, timestamp)
				if err != nil {
					return fmt.Errorf("failed to get exchange rate: %w", err)
				}

				maturedHeight := height - maturityDelay
				log.Debug("expiring contracts", zap.Uint64("maturedHeight", maturedHeight))
				// apply payouts
				expiredContracts, err := missedContracts(tx, maturedHeight)
				if err != nil {
					return fmt.Errorf("failed to get expired contracts: %w", err)
				}
				missed = len(expiredContracts)

				for _, c := range expiredContracts {
					var revenue stats.Values
					v, underflow := c.FinalMissed.SubWithUnderflow(c.InitialMissed) // calculate the revenue from revisions
					if !underflow {
						revenue.SC = v.Add(c.InitialMissedRevenue) // add the initial revenue from a renewal
						sc := decimal.NewFromBigInt(revenue.SC.Big(), -24)
						revenue.USD = sc.Mul(usdRate)
						revenue.EUR = sc.Mul(eurRate)
						revenue.BTC = sc.Mul(btcRate)
					}

					// add the revenue to the total
					totalRevenue = totalRevenue.Add(revenue)
					// add the missed payout to the total
					var payout stats.Values
					payout.SC = c.FinalValid
					sc := decimal.NewFromBigInt(payout.SC.Big(), -24)
					payout.USD = sc.Mul(usdRate)
					payout.EUR = sc.Mul(eurRate)
					payout.BTC = sc.Mul(btcRate)

					totalPayout = totalPayout.Add(payout)

					log.Debug("missed contract", zap.Stringer("contractID", c.ID), zap.String("payout", c.FinalMissed.ExactString()), zap.String("revenue", revenue.SC.ExactString()), zap.Stringer("revenueUSD", revenue.USD), zap.Stringer("exchangeRateUSD", usdRate))
				}

				successfulContracts, err := validContracts(tx, maturedHeight)
				if err != nil {
					return fmt.Errorf("failed to get proven contracts: %w", err)
				}
				valid = len(successfulContracts)

				for _, c := range successfulContracts {
					var revenue stats.Values
					v, underflow := c.FinalValid.SubWithUnderflow(c.InitialValid) // calculate the revenue from revisions
					if !underflow {
						revenue.SC = v.Add(c.InitialValidRevenue) // add the initial revenue from a renewal
						sc := decimal.NewFromBigInt(revenue.SC.Big(), -24)
						revenue.USD = sc.Mul(usdRate)
						revenue.EUR = sc.Mul(eurRate)

					}
					// add the revenue to the total
					totalRevenue.SC = totalRevenue.SC.Add(revenue.SC)
					totalRevenue.USD = totalRevenue.USD.Add(revenue.USD)
					totalRevenue.EUR = totalRevenue.EUR.Add(revenue.EUR)
					totalRevenue.BTC = totalRevenue.BTC.Add(revenue.BTC)

					// add the valid payout to the total
					var payout stats.Values
					payout.SC = c.FinalValid
					sc := decimal.NewFromBigInt(payout.SC.Big(), -24)
					payout.USD = sc.Mul(usdRate)
					payout.EUR = sc.Mul(eurRate)
					payout.BTC = sc.Mul(btcRate)

					totalPayout.SC = totalPayout.SC.Add(payout.SC)
					totalPayout.USD = totalPayout.USD.Add(payout.USD)
					totalPayout.EUR = totalPayout.EUR.Add(payout.EUR)
					totalPayout.BTC = totalPayout.BTC.Add(payout.BTC)

					log.Debug("valid contract", zap.Stringer("contractID", c.ID), zap.String("payout", c.FinalValid.ExactString()), zap.String("revenue", revenue.SC.ExactString()), zap.Stringer("revenueUSD", revenue.USD), zap.Stringer("exchangeRateUSD", usdRate))
				}
			}

			if err := updateContractStats(tx, active-valid-missed, valid, missed, totalRevenue, totalPayout, timestamp); err != nil {
				return fmt.Errorf("failed to update contract stats: %w", err)
			}

			height++
			log.Debug("applied block", zap.Stringer("blockID", blockID), zap.Time("timestamp", timestamp))
		}

		if uint64(cc.BlockHeight) > maturityDelay {
			if err := deleteExpired(tx, uint64(cc.BlockHeight)-maturityDelay); err != nil {
				return fmt.Errorf("failed to delete expired contracts: %w", err)
			}
		}

		if err := setLastChange(tx, cc.ID, uint64(cc.BlockHeight)); err != nil {
			return fmt.Errorf("failed to set last change: %w", err)
		}
		return nil
	})
	if err != nil {
		log.Panic("failed to process consensus change", zap.Error(err))
	}
}

func sum(values []types.Currency) (t types.Currency) {
	for _, v := range values {
		t = t.Add(v)
	}
	return
}

func estimateHostFunds(inputs, outputs []types.Currency, renterTarget, hostTarget types.Currency) (types.Currency, bool) {
	// this is naive, but it attempts to separate the renter and host inputs
	// and outputs using the estimated funding amounts for each party as a
	// guide.
	for i := range inputs {
		renterInput, hostInput := sum(inputs[:i]), sum(inputs[i:])

		for j := len(outputs); j >= 0; j-- {
			renterOutput, hostOutput := sum(outputs[:j]), sum(outputs[j:])

			// SUM(renter inputs) - SUM(renter outputs) should be greater than
			// the miner fees + renter valid payout since the renter also pays
			// the contract fee + initial revenue to the host payout
			//
			// SUM(host inputs) - SUM(host outputs) should be less than the host
			// missed payout since the renter includes the contract fee.
			if renterInput.Cmp(renterOutput) < 0 || hostInput.Cmp(hostOutput) < 0 {
				continue
			} else if renterInput.Sub(renterOutput).Cmp(renterTarget) <= 0 || hostInput.Sub(hostOutput).Cmp(hostTarget) >= 0 {
				continue
			}
			return hostInput.Sub(hostOutput), true
		}
	}
	return types.ZeroCurrency, false
}

func setLastChange(tx txn, ccID modules.ConsensusChangeID, height uint64) error {
	_, err := tx.Exec(`UPDATE global_settings SET contracts_last_processed_change=$1, contracts_height=$2`, sqlHash256(ccID), height)
	return err
}

func deleteExpired(tx txn, height uint64) error {
	_, err := tx.Exec(`DELETE FROM active_contracts WHERE expiration_height <= $1`, height)
	if err != nil {
		return fmt.Errorf("failed to delete expired contracts: %w", err)
	}
	const query = `DELETE FROM active_contracts WHERE proof_block_id IN (SELECT id FROM blocks WHERE height <= $1)`
	if _, err := tx.Exec(query, height); err != nil {
		return fmt.Errorf("failed to delete proven contracts: %w", err)
	}
	return nil
}

func revertBlock(tx txn, blockID types.BlockID) error {
	var blockDBID int64
	err := tx.QueryRow(`SELECT id FROM blocks WHERE block_id=$1`, sqlHash256(blockID)).Scan(&blockDBID)
	if err != nil {
		return fmt.Errorf("failed to get block id: %w", err)
	}

	// clear contract references to this block
	_, err = tx.Exec(`UPDATE active_contracts SET proof_block_id=NULL WHERE proof_block_id=$1`, blockDBID)
	if err != nil {
		return fmt.Errorf("failed to update active contracts: %w", err)
	}

	_, err = tx.Exec(`DELETE FROM active_contracts WHERE block_id=$1`, blockDBID)
	if err != nil {
		return fmt.Errorf("failed to delete active contracts: %w", err)
	}

	_, err = tx.Exec(`DELETE FROM blocks WHERE id=$1`, blockDBID)
	return err
}

func addBlock(tx txn, blockID types.BlockID, height uint64, timestamp time.Time) (id int64, err error) {
	err = tx.QueryRow(`INSERT INTO blocks (block_id, height, date_created) VALUES ($1, $2, $3) RETURNING id`, sqlHash256(blockID), height, sqlTime(timestamp)).Scan(&id)
	return
}

func addActiveContract(tx txn, id types.FileContractID, fc types.FileContract, blockID int64, initialValidRevenue, initialMissedRevenue types.Currency) error {
	var initialValid, initialMissed types.Currency
	if len(fc.ValidProofOutputs) >= 2 {
		initialValid = fc.ValidHostPayout()
	}

	if len(fc.MissedProofOutputs) >= 2 {
		initialMissed = fc.MissedHostPayout()
	}

	var expirationHeight int64
	if fc.WindowEnd > math.MaxInt64 {
		expirationHeight = math.MaxInt64
	} else {
		expirationHeight = int64(fc.WindowEnd)
	}

	_, err := tx.Exec(`INSERT INTO active_contracts (contract_id, block_id, valid_payout_value, missed_payout_value, initial_valid_payout_value, initial_missed_payout_value, initial_valid_revenue, initial_missed_revenue, expiration_height)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`, sqlHash256(id), blockID, sqlCurrency(initialValid), sqlCurrency(initialMissed), sqlCurrency(initialValid), sqlCurrency(initialMissed), sqlCurrency(initialValidRevenue), sqlCurrency(initialMissedRevenue), expirationHeight)
	return err
}

func reviseContract(tx txn, id types.FileContractID, validPayout, missedPayout types.Currency) error {
	_, err := tx.Exec(`UPDATE active_contracts SET (valid_payout_value, missed_payout_value) = ($1, $2) WHERE contract_id=$3`, sqlCurrency(validPayout), sqlCurrency(missedPayout), sqlHash256(id))
	return err
}

func proveContract(tx txn, id types.FileContractID, blockID int64) error {
	var dbID int64
	err := tx.QueryRow(`UPDATE active_contracts SET proof_block_id=$1 WHERE contract_id=$2 RETURNING id`, blockID, sqlHash256(id)).Scan(&dbID)
	return err
}

func updateContractStats(tx txn, active, valid, missed int, revenue, payout stats.Values, timestamp time.Time) error {
	if active == 0 && valid == 0 && missed == 0 {
		return nil
	}

	state, err := getMetrics(tx, timestamp)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to get contract stats: %w", err)
	}

	state.Active += active
	state.Valid += valid
	state.Missed += missed
	state.Revenue = state.Revenue.Add(revenue)
	state.Payout = state.Payout.Add(payout)

	if state.Active < 0 {
		return fmt.Errorf("invalid active contract count: %d", state.Active)
	} else if state.Valid < 0 {
		return fmt.Errorf("invalid valid contract count: %d", state.Valid)
	} else if state.Missed < 0 {
		return fmt.Errorf("invalid missed contract count: %d", state.Missed)
	}

	const upsertQuery = `INSERT INTO hourly_contract_stats (date_created, active_contracts, 
valid_contracts, missed_contracts, total_payouts_sc, total_payouts_usd, total_payouts_eur, total_payouts_btc,
estimated_revenue_sc, estimated_revenue_usd, estimated_revenue_eur, estimated_revenue_btc) 
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,  $11, $12)
ON CONFLICT (date_created) DO UPDATE SET active_contracts=EXCLUDED.active_contracts, valid_contracts=EXCLUDED.valid_contracts,
missed_contracts=EXCLUDED.missed_contracts, total_payouts_sc=EXCLUDED.total_payouts_sc, total_payouts_usd=EXCLUDED.total_payouts_usd,
total_payouts_eur=EXCLUDED.total_payouts_eur, total_payouts_btc=EXCLUDED.total_payouts_btc, estimated_revenue_sc=EXCLUDED.estimated_revenue_sc,
estimated_revenue_usd=EXCLUDED.estimated_revenue_usd, estimated_revenue_eur=EXCLUDED.estimated_revenue_eur, estimated_revenue_btc=EXCLUDED.estimated_revenue_btc`

	_, err = tx.Exec(upsertQuery, sqlTime(timestamp), state.Active, state.Valid, state.Missed,
		sqlCurrency(state.Payout.SC),
		state.Payout.USD,
		state.Payout.EUR,
		state.Payout.BTC,
		sqlCurrency(state.Revenue.SC),
		state.Revenue.USD,
		state.Revenue.EUR,
		state.Revenue.BTC)
	return err
}

func missedContracts(tx txn, height uint64) (contracts []stats.Contract, err error) {
	const query = `SELECT c.contract_id, b.block_id, c.initial_valid_payout_value,
c.initial_missed_payout_value, c.valid_payout_value, c.missed_payout_value,
c.initial_valid_revenue, c.initial_missed_revenue, c.expiration_height, 0
FROM active_contracts c
INNER JOIN blocks b ON c.block_id=b.id
WHERE c.expiration_height <= $1 AND c.proof_block_id IS NULL`
	rows, err := tx.Query(query, height)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		c, err := scanContract(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan missed contract: %w", err)
		}
		contracts = append(contracts, c)
	}
	return contracts, nil
}

func validContracts(tx txn, height uint64) (contracts []stats.Contract, err error) {
	const query = `SELECT c.contract_id, b.block_id, c.initial_valid_payout_value,
c.initial_missed_payout_value, c.valid_payout_value, c.missed_payout_value,
c.initial_valid_revenue, c.initial_missed_revenue, c.expiration_height, 0
FROM active_contracts c
INNER JOIN blocks b ON c.block_id=b.id
INNER JOIN blocks pb ON c.proof_block_id=pb.id
WHERE pb.height <= $1`
	rows, err := tx.Query(query, height)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		c, err := scanContract(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan missed contract: %w", err)
		}
		contracts = append(contracts, c)
	}
	return contracts, nil
}

func scanContract(row scanner) (c stats.Contract, err error) {
	err = row.Scan((*sqlHash256)(&c.ID), (*sqlHash256)(&c.BlockID),
		(*sqlCurrency)(&c.InitialValid), (*sqlCurrency)(&c.InitialMissed),
		(*sqlCurrency)(&c.FinalValid), (*sqlCurrency)(&c.FinalMissed),
		(*sqlCurrency)(&c.InitialValidRevenue), (*sqlCurrency)(&c.InitialMissedRevenue),
		&c.ExpirationHeight, &c.ProofHeight)
	return
}

func convertToCore(siad encoding.SiaMarshaler, core types.DecoderFrom) {
	var buf bytes.Buffer
	siad.MarshalSia(&buf)
	d := types.NewBufDecoder(buf.Bytes())
	core.DecodeFrom(d)
	if d.Err() != nil {
		panic(d.Err())
	}
}
