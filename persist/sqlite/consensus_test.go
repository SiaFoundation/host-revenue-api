package sqlite_test

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.sia.tech/contract-revenue/internal/chain"
	"go.sia.tech/contract-revenue/internal/test"
	"go.sia.tech/contract-revenue/persist/sqlite"
	rhp2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/modules/consensus"
	"go.sia.tech/siad/modules/gateway"
	"go.sia.tech/siad/modules/transactionpool"
	stypes "go.sia.tech/siad/types"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestIndexing(t *testing.T) {
	log := zaptest.NewLogger(t)
	dir := t.TempDir()

	g, err := gateway.New(":0", false, filepath.Join(dir, "gateway"))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	cs, errCh := consensus.New(g, false, filepath.Join(dir, "consensus"))
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	default:
		go func() {
			if err := <-errCh; err != nil && !strings.Contains(err.Error(), "ThreadGroup already stopped") {
				panic(err)
			}
		}()
	}
	defer cs.Close()

	cm, err := chain.NewManager(cs)
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	stp, err := transactionpool.New(cs, g, filepath.Join(dir, "tpool"))
	if err != nil {
		t.Fatal(err)
	}
	defer stp.Close()
	tp := chain.NewTPool(stp)

	w := test.NewWallet()
	if err := cs.ConsensusSetSubscribe(w, modules.ConsensusChangeBeginning, nil); err != nil {
		t.Fatal(err)
	}

	db, err := sqlite.OpenDatabase(filepath.Join(dir, "test.db"), log)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := cs.ConsensusSetSubscribe(db, modules.ConsensusChangeBeginning, nil); err != nil {
		t.Fatal(err)
	}

	miner := test.NewMiner(cm)
	if err := cs.ConsensusSetSubscribe(miner, modules.ConsensusChangeBeginning, nil); err != nil {
		t.Fatal(err)
	}
	tp.Subscribe(miner)

	// mine until the wallet has funds and all forks have been resolved
	if err := miner.Mine(w.Address(), int(stypes.MaturityDelay)*4); err != nil {
		t.Fatal(err)
	}

	renterKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	hostKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))

	endHeight := cm.TipState().Index.Height + 20
	// minimal rhp2 settings for contract formation
	hostSettings := rhp2.HostSettings{
		WindowSize:    10,
		ContractPrice: types.Siacoins(1).Div64(4),
	}
	fc := rhp2.PrepareContractFormation(renterKey.PublicKey(), hostKey.PublicKey(), types.Siacoins(100), types.Siacoins(200), endHeight, hostSettings, w.Address())
	// add a contract
	fc1Txn := types.Transaction{
		FileContracts: []types.FileContract{fc},
	}

	toSign, release, err := w.FundTransaction(&fc1Txn, fc.Payout)
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	if err := w.Sign(&fc1Txn, cm.TipState(), toSign, types.CoveredFields{WholeTransaction: true}); err != nil {
		t.Fatal(err)
	} else if err := tp.AcceptTransactionSet([]types.Transaction{fc1Txn}); err != nil {
		t.Fatal(err)
	}

	// mine a block to confirm the contract
	if err := miner.Mine(w.Address(), 1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second) // sync time

	// check the statistics were updated
	stats, err := db.Metrics(time.Now())
	if err != nil {
		t.Fatal(err)
	} else if stats.Active != 1 {
		t.Fatal("expected 1 active contracts, got", stats.Active)
	} else if stats.Missed != 0 {
		t.Fatal("expected 0 missed contracts, got", stats.Missed)
	} else if stats.Valid != 0 {
		t.Fatal("expected 0 valid contracts, got", stats.Valid)
	} else if !stats.Payout.IsZero() {
		t.Fatal("expected payout to be zero, got", stats.Payout)
	} else if !stats.Revenue.IsZero() {
		t.Fatal("expected revenue to be zero, got", stats.Revenue)
	}

	// add a second contract
	endHeight = cm.TipState().Index.Height + 30
	fc2 := rhp2.PrepareContractFormation(renterKey.PublicKey(), hostKey.PublicKey(), types.Siacoins(150), types.Siacoins(300), endHeight, hostSettings, w.Address())
	// add a contract
	fc2Txn := types.Transaction{
		FileContracts: []types.FileContract{fc2},
	}

	toSign, release, err = w.FundTransaction(&fc2Txn, fc2.Payout)
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	if err := w.Sign(&fc2Txn, cm.TipState(), toSign, types.CoveredFields{WholeTransaction: true}); err != nil {
		t.Fatal(err)
	} else if err := tp.AcceptTransactionSet([]types.Transaction{fc2Txn}); err != nil {
		t.Fatal(err)
	}

	fc2ID := fc2Txn.FileContractID(0)

	// mine a block to confirm the contract
	if err := miner.Mine(w.Address(), 1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second) // sync time

	// check the statistics were updated
	stats, err = db.Metrics(time.Now())
	if err != nil {
		t.Fatal(err)
	} else if stats.Active != 2 {
		t.Fatal("expected 2 active contracts, got", stats.Active)
	} else if stats.Missed != 0 {
		t.Fatal("expected 0 missed contracts, got", stats.Missed)
	} else if stats.Valid != 0 {
		t.Fatal("expected 0 valid contracts, got", stats.Valid)
	} else if !stats.Payout.IsZero() {
		t.Fatal("expected payout to be zero, got", stats.Payout)
	} else if !stats.Revenue.IsZero() {
		t.Fatal("expected revenue to be zero, got", stats.Revenue)
	}

	// submit a revision transferring some of the renter funds from the second contract to the host
	transfer, collateral := types.Siacoins(50), types.Siacoins(10)
	revFC2 := types.FileContractRevision{
		ParentID: fc2ID,
		UnlockConditions: types.UnlockConditions{
			PublicKeys: []types.UnlockKey{
				renterKey.PublicKey().UnlockKey(),
				hostKey.PublicKey().UnlockKey(),
			},
			SignaturesRequired: 2,
		},
		FileContract: fc2,
	}
	revFC2.RevisionNumber = 1
	// transfer the funds to the host on success
	revFC2.ValidProofOutputs[0].Value = revFC2.ValidProofOutputs[0].Value.Sub(transfer)
	revFC2.ValidProofOutputs[1].Value = revFC2.ValidProofOutputs[1].Value.Add(transfer)
	// burn the funds on failure
	revFC2.MissedProofOutputs[0].Value = revFC2.MissedProofOutputs[0].Value.Sub(transfer)
	revFC2.MissedProofOutputs[1].Value = revFC2.MissedProofOutputs[1].Value.Sub(collateral)
	revFC2.MissedProofOutputs[2].Value = revFC2.MissedProofOutputs[2].Value.Add(transfer).Add(collateral)

	revFC2Txn := types.Transaction{
		FileContractRevisions: []types.FileContractRevision{revFC2},
		Signatures: []types.TransactionSignature{
			{
				ParentID: types.Hash256(fc2ID),
				CoveredFields: types.CoveredFields{
					FileContractRevisions: []uint64{0},
				},
				PublicKeyIndex: 0,
			},
			{
				ParentID: types.Hash256(fc2ID),
				CoveredFields: types.CoveredFields{
					FileContractRevisions: []uint64{0},
				},
				PublicKeyIndex: 1,
			},
		},
	}
	state := cm.TipState()
	sigHash := state.PartialSigHash(revFC2Txn, types.CoveredFields{FileContractRevisions: []uint64{0}})
	renterSig := renterKey.SignHash(sigHash)
	hostSig := hostKey.SignHash(sigHash)
	revFC2Txn.Signatures[0].Signature = renterSig[:]
	revFC2Txn.Signatures[1].Signature = hostSig[:]

	if err := tp.AcceptTransactionSet([]types.Transaction{revFC2Txn}); err != nil {
		t.Fatal(err)
	}

	// mine until the first contract expires
	expirationHeight := int(fc.WindowEnd-cm.TipState().Index.Height+uint64(stypes.MaturityDelay)) + 1
	if err := miner.Mine(w.Address(), expirationHeight); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second) // sync time

	expectedPayout := fc.MissedProofOutputs[1].Value

	// check the statistics were updated
	stats, err = db.Metrics(time.Now())
	if err != nil {
		t.Fatal(err)
	} else if stats.Active != 1 {
		t.Fatal("expected 1 active contracts, got", stats.Active)
	} else if stats.Missed != 1 {
		t.Fatal("expected 1 missed contracts, got", stats.Missed)
	} else if stats.Valid != 0 {
		t.Fatal("expected 0 valid contracts, got", stats.Valid)
	} else if !stats.Payout.Equals(expectedPayout) {
		t.Fatalf("expected payout to be %d, got %d", expectedPayout, stats.Payout)
	} else if !stats.Revenue.IsZero() {
		t.Fatal("expected revenue to be zero, got", stats.Revenue)
	}

	// mine until the second contract expires
	expirationHeight = int(fc2.WindowEnd-cm.TipState().Index.Height+uint64(stypes.MaturityDelay)) + 1
	if err := miner.Mine(w.Address(), expirationHeight); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second) // sync time

	expectedPayout = expectedPayout.Add(revFC2.MissedProofOutputs[1].Value)
	// check the statistics were updated
	stats, err = db.Metrics(time.Now())
	if err != nil {
		t.Fatal(err)
	} else if stats.Active != 0 {
		t.Fatal("expected 2 active contracts, got", stats.Active)
	} else if stats.Missed != 2 {
		t.Fatal("expected 2 missed contracts, got", stats.Missed)
	} else if stats.Valid != 0 {
		t.Fatal("expected 0 valid contracts, got", stats.Valid)
	} else if !stats.Payout.Equals(expectedPayout) {
		t.Fatalf("expected payout to be %d, got %d", expectedPayout, stats.Payout)
	} else if !stats.Revenue.IsZero() {
		t.Fatal("expected revenue to be zero, got", stats.Revenue)
	}

	// add a third contract
	endHeight = cm.TipState().Index.Height + 30
	fc3 := rhp2.PrepareContractFormation(renterKey.PublicKey(), hostKey.PublicKey(), types.Siacoins(150), types.Siacoins(300), endHeight, hostSettings, w.Address())
	// add a contract
	fc3Txn := types.Transaction{
		FileContracts: []types.FileContract{fc3},
	}

	toSign, release, err = w.FundTransaction(&fc3Txn, fc3.Payout)
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	if err := w.Sign(&fc3Txn, cm.TipState(), toSign, types.CoveredFields{WholeTransaction: true}); err != nil {
		t.Fatal(err)
	} else if err := tp.AcceptTransactionSet([]types.Transaction{fc3Txn}); err != nil {
		t.Fatal(err)
	}

	fc3ID := fc3Txn.FileContractID(0)
	fc3InitialPayout := fc3.ValidHostPayout()

	// mine a block to confirm the contract
	if err := miner.Mine(w.Address(), 1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second) // sync time

	// check the statistics were updated
	stats, err = db.Metrics(time.Now())
	if err != nil {
		t.Fatal(err)
	} else if stats.Active != 1 {
		t.Fatal("expected 1 active contracts, got", stats.Active)
	} else if stats.Missed != 2 {
		t.Fatal("expected 2 missed contracts, got", stats.Missed)
	} else if stats.Valid != 0 {
		t.Fatal("expected 0 valid contracts, got", stats.Valid)
	} else if !stats.Payout.Equals(expectedPayout) {
		t.Fatalf("expected payout to be %d, got %d", expectedPayout, stats.Payout)
	} else if !stats.Revenue.IsZero() {
		t.Fatal("expected revenue to be zero, got", stats.Revenue)
	}

	// submit a revision transferring some of the renter funds from the second contract to the host
	transfer, collateral = types.Siacoins(50), types.Siacoins(10)
	revFC3 := types.FileContractRevision{
		ParentID: fc3ID,
		UnlockConditions: types.UnlockConditions{
			PublicKeys: []types.UnlockKey{
				renterKey.PublicKey().UnlockKey(),
				hostKey.PublicKey().UnlockKey(),
			},
			SignaturesRequired: 2,
		},
		FileContract: fc3,
	}
	revFC3.RevisionNumber = 1
	// transfer the funds to the host on success
	revFC3.ValidProofOutputs[0].Value = revFC3.ValidProofOutputs[0].Value.Sub(transfer)
	revFC3.ValidProofOutputs[1].Value = revFC3.ValidProofOutputs[1].Value.Add(transfer)
	// burn the funds on failure
	revFC3.MissedProofOutputs[0].Value = revFC3.MissedProofOutputs[0].Value.Sub(transfer)
	revFC3.MissedProofOutputs[1].Value = revFC3.MissedProofOutputs[1].Value.Sub(collateral)
	revFC3.MissedProofOutputs[2].Value = revFC3.MissedProofOutputs[2].Value.Add(transfer).Add(collateral)

	rev3Txn := types.Transaction{
		FileContractRevisions: []types.FileContractRevision{revFC3},
		Signatures: []types.TransactionSignature{
			{
				ParentID: types.Hash256(fc3ID),
				CoveredFields: types.CoveredFields{
					FileContractRevisions: []uint64{0},
				},
				PublicKeyIndex: 0,
			},
			{
				ParentID: types.Hash256(fc3ID),
				CoveredFields: types.CoveredFields{
					FileContractRevisions: []uint64{0},
				},
				PublicKeyIndex: 1,
			},
		},
	}
	state = cm.TipState()
	sigHash = state.PartialSigHash(rev3Txn, types.CoveredFields{FileContractRevisions: []uint64{0}})
	renterSig = renterKey.SignHash(sigHash)
	hostSig = hostKey.SignHash(sigHash)
	rev3Txn.Signatures[0].Signature = renterSig[:]
	rev3Txn.Signatures[1].Signature = hostSig[:]

	if err := tp.AcceptTransactionSet([]types.Transaction{rev3Txn}); err != nil {
		t.Fatal(err)
	}

	// mine until the proof window of the third contract
	proofWindowStart := int(fc3.WindowStart-cm.TipState().Index.Height) + 1
	if err := miner.Mine(w.Address(), proofWindowStart); err != nil {
		t.Fatal(err)
	}

	// submit a storage proof for the third contract
	// since there is no data an empty proof is valid
	proofTxn := types.Transaction{
		StorageProofs: []types.StorageProof{
			{
				ParentID: fc3ID,
			},
		},
	}

	if err := tp.AcceptTransactionSet([]types.Transaction{proofTxn}); err != nil {
		t.Fatal(err)
	}

	// mine until the third contract payout is available
	expirationHeight = proofWindowStart + int(stypes.MaturityDelay) + 1
	if err := miner.Mine(w.Address(), expirationHeight); err != nil {
		t.Fatal(err)
	}

	expectedPayout = expectedPayout.Add(revFC3.ValidProofOutputs[1].Value)
	expectedRevenue := revFC3.ValidHostPayout().Sub(fc3InitialPayout)

	// check the statistics were updated
	stats, err = db.Metrics(time.Now())
	if err != nil {
		t.Fatal(err)
	} else if stats.Active != 0 {
		t.Fatal("expected 0 active contracts, got", stats.Active)
	} else if stats.Missed != 2 {
		t.Fatal("expected 2 missed contracts, got", stats.Missed)
	} else if stats.Valid != 1 {
		t.Fatal("expected 1 valid contract, got", stats.Valid)
	} else if !stats.Payout.Equals(expectedPayout) {
		t.Fatalf("expected payout to be %d, got %d", expectedPayout, stats.Payout)
	} else if !stats.Revenue.Equals(expectedRevenue) {
		t.Fatalf("expected revenue to be %d, got %d", expectedRevenue, stats.Revenue)
	}

}
