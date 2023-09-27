package sqlite

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

	"gitlab.com/NebulousLabs/encoding"
	"go.sia.tech/contract-revenue/stats"
	"go.sia.tech/core/types"
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
		height := uint64(cc.BlockHeight) - uint64(len(cc.AppliedBlocks)) + 1
		for _, applied := range cc.AppliedBlocks {
			blockID := types.BlockID(applied.ID())
			timestamp := time.Unix(int64(applied.Timestamp), 0)
			log.Debug("adding block", zap.Stringer("blockID", blockID), zap.Time("timestamp", timestamp), zap.Uint64("height", height))
			blockDBID, err := addBlock(tx, blockID, height)
			if err != nil {
				return fmt.Errorf("failed to add block %q: %w", applied.ID(), err)
			}

			var active int
			for _, txn := range applied.Transactions {
				for i, fc := range txn.FileContracts {
					fcID := types.FileContractID(txn.FileContractID(uint64(i)))

					var contract types.FileContract
					convertToCore(fc, &contract)

					if err := addActiveContract(tx, fcID, contract, blockDBID); err != nil {
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
			var revenue, payout types.Currency
			if height > maturityDelay {
				maturedHeight := height - maturityDelay
				log.Debug("expiring contracts", zap.Uint64("maturedHeight", maturedHeight))
				// apply payouts
				expiredContracts, err := missedContracts(tx, maturedHeight)
				if err != nil {
					return fmt.Errorf("failed to get expired contracts: %w", err)
				}
				missed = len(expiredContracts)

				for _, c := range expiredContracts {
					payout = payout.Add(c.FinalMissed)
					v, underflow := payout.SubWithUnderflow(c.InitialMissed)
					if !underflow {
						revenue = revenue.Add(v)
					}
					log.Debug("missed contract", zap.Stringer("contractID", c.ID), zap.String("payout", c.FinalMissed.ExactString()), zap.String("revenue", v.ExactString()))
				}

				successfulContracts, err := validContracts(tx, maturedHeight)
				if err != nil {
					return fmt.Errorf("failed to get proven contracts: %w", err)
				}
				valid = len(successfulContracts)

				for _, c := range successfulContracts {
					payout = payout.Add(c.FinalValid)
					v, underflow := payout.SubWithUnderflow(c.InitialValid)
					if !underflow {
						revenue = revenue.Add(v)
					}
					log.Debug("valid contract", zap.Stringer("contractID", c.ID), zap.String("payout", c.FinalValid.ExactString()), zap.String("revenue", v.ExactString()))
				}
			}

			if err := updateContractStats(tx, active-valid-missed, valid, missed, revenue, payout, timestamp); err != nil {
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

func addBlock(tx txn, blockID types.BlockID, height uint64) (id int64, err error) {
	err = tx.QueryRow(`INSERT INTO blocks (block_id, height) VALUES ($1, $2) RETURNING id`, sqlHash256(blockID), height).Scan(&id)
	return
}

func addActiveContract(tx txn, id types.FileContractID, fc types.FileContract, blockID int64) error {
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

	_, err := tx.Exec(`INSERT INTO active_contracts (contract_id, block_id, valid_payout_value, missed_payout_value, initial_valid_payout_value, initial_missed_payout_value, expiration_height)
VALUES ($1, $2, $3, $4, $5, $6, $7)`, sqlHash256(id), blockID, sqlCurrency(initialValid), sqlCurrency(initialMissed), sqlCurrency(initialValid), sqlCurrency(initialMissed), expirationHeight)
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

func updateContractStats(tx txn, active, valid, missed int, revenue, payout types.Currency, timestamp time.Time) error {
	if active == 0 && valid == 0 && missed == 0 && revenue.IsZero() && payout.IsZero() {
		return nil
	}

	timestamp = timestamp.Truncate(time.Hour)
	const query = `SELECT active_contracts, valid_contracts, missed_contracts, total_payouts, estimated_revenue FROM hourly_contract_stats WHERE date_created <= $1 ORDER BY date_created DESC LIMIT 1`
	var totalActive, totalValid, totalMissed int
	var totalRevenue, totalPayout types.Currency

	err := tx.QueryRow(query, timestamp).Scan(&totalActive, &totalValid, &totalMissed, (*sqlCurrency)(&totalPayout), (*sqlCurrency)(&totalRevenue))
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to get contract stats: %w", err)
	}

	totalActive += active
	totalValid += valid
	totalMissed += missed

	if totalActive < 0 {
		return fmt.Errorf("invalid active contract count: %d", totalActive)
	} else if totalValid < 0 {
		return fmt.Errorf("invalid valid contract count: %d", totalValid)
	} else if totalMissed < 0 {
		return fmt.Errorf("invalid missed contract count: %d", totalMissed)
	}

	totalRevenue = totalRevenue.Add(revenue)
	totalPayout = totalPayout.Add(payout)

	const upsertQuery = `INSERT INTO hourly_contract_stats (date_created, active_contracts, 
		valid_contracts, missed_contracts, total_payouts, estimated_revenue) VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (date_created) DO UPDATE SET active_contracts=$2, valid_contracts=$3, missed_contracts=$4, total_payouts=$5, estimated_revenue=$6`

	_, err = tx.Exec(upsertQuery, sqlTime(timestamp), totalActive, totalValid, totalMissed, sqlCurrency(totalPayout), sqlCurrency(totalRevenue))
	return err
}

func missedContracts(tx txn, height uint64) (contracts []stats.Contract, err error) {
	const query = `SELECT c.contract_id, b.block_id, c.initial_valid_payout_value,
c.initial_missed_payout_value, c.valid_payout_value, c.missed_payout_value,
c.expiration_height, 0
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
c.expiration_height, 0
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
