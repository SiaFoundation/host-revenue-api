package test

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"go.sia.tech/contract-revenue/internal/chain"
	"go.sia.tech/core/types"
	"go.sia.tech/siad/modules"
	mconsensus "go.sia.tech/siad/modules/consensus"
	"go.sia.tech/siad/modules/gateway"
	"go.sia.tech/siad/modules/transactionpool"
	stypes "go.sia.tech/siad/types"
	"lukechampine.com/frand"
)

// TestMining tests that cpu mining works as expected and adds spendable outputs
// to the wallet.
func TestMining(t *testing.T) {
	dir := t.TempDir()

	g, err := gateway.New("localhost:0", false, filepath.Join(dir, modules.GatewayDir))
	if err != nil {
		t.Fatal("could not create gateway:", err)
	}
	t.Cleanup(func() { g.Close() })

	cs, errChan := mconsensus.New(g, false, filepath.Join(dir, modules.ConsensusDir))
	if err := <-errChan; err != nil {
		t.Fatal("could not create consensus set:", err)
	}
	go func() {
		for err := range errChan {
			panic(fmt.Errorf("consensus err: %w", err))
		}
	}()
	defer cs.Close()

	cm, err := chain.NewManager(cs)
	if err != nil {
		t.Fatal("could not create chain manager:", err)
	}
	defer cm.Close()

	stp, err := transactionpool.New(cs, g, filepath.Join(dir, modules.TransactionPoolDir))
	if err != nil {
		t.Fatal("could not create tpool:", err)
	}
	defer stp.Close()
	tp := chain.NewTPool(stp)

	w := NewWallet()
	if err := cs.ConsensusSetSubscribe(w, modules.ConsensusChangeBeginning, nil); err != nil {
		t.Fatal("failed to subscribe to consensus set:", err)
	}

	m := NewMiner(cm)
	if err := cs.ConsensusSetSubscribe(m, modules.ConsensusChangeBeginning, nil); err != nil {
		t.Fatal("failed to subscribe to consensus set:", err)
	}
	tp.Subscribe(m)

	// mine a single block
	if err := m.Mine(w.Address(), 1); err != nil {
		t.Fatal(err)
	}

	// make sure the block height is updated
	if height := cs.Height(); height != 1 {
		t.Fatalf("expected height 1, got %v", height)
	}

	// mine until the maturity height of the first payout is reached
	if err := m.Mine(w.Address(), int(stypes.MaturityDelay)); err != nil {
		t.Fatal(err)
	} else if height := cs.Height(); height != stypes.MaturityDelay+1 {
		t.Fatalf("expected height %v, got %v", stypes.MaturityDelay+1, height)
	}

	// make sure we have the expected balance
	siadExpectedBalance := stypes.CalculateCoinbase(1)
	var expectedBalance types.Currency
	convertToCore(siadExpectedBalance, &expectedBalance)
	if _, balance := w.Balance(); !balance.Equals(expectedBalance) {
		t.Fatalf("expected balance to be %v, got %v", expectedBalance, balance)
	}

	// mine more blocks until we have lots of outputs
	if err := m.Mine(w.Address(), 100); err != nil {
		t.Fatal(err)
	}

	// add random transactions to the tpool
	added := make([]types.TransactionID, 100)
	for i := range added {
		amount := types.Siacoins(uint32(1 + frand.Intn(1000)))
		txn := types.Transaction{
			ArbitraryData: [][]byte{append(modules.PrefixNonSia[:], frand.Bytes(16)...)},
			SiacoinOutputs: []types.SiacoinOutput{
				{Value: amount},
			},
		}

		toSign, release, err := w.FundTransaction(&txn, amount)
		if err != nil {
			t.Fatal(err)
		}
		defer release()

		if err := w.Sign(&txn, cm.TipState(), toSign, types.CoveredFields{WholeTransaction: true}); err != nil {
			t.Fatal(err)
		}

		if err := tp.AcceptTransactionSet([]types.Transaction{txn}); err != nil {
			buf, _ := json.MarshalIndent(txn, "", "  ")
			t.Log(string(buf))
			t.Fatalf("failed to accept transaction %v: %v", i, err)
		}

		added[i] = txn.ID()
	}

	// mine a block to confirm the transactions
	if err := m.Mine(w.Address(), 1); err != nil {
		t.Fatal(err)
	}

	// check that the correct number of transactions are in the block. A random
	// transaction is added before all others.
	block, ok := cm.BlockAtHeight(cm.TipState().Index.Height)
	if !ok {
		t.Fatal("block not found")
	}
	if len(block.Transactions) != len(added)+1 {
		t.Fatalf("expected %v transactions, got %v", len(added), len(block.Transactions))
	}
	// the first transaction in the block should be ignored
	for i, txn := range block.Transactions[1:] {
		if txn.ID() != added[i] {
			t.Fatalf("transaction %v expected ID %v, got %v", i, added[i], txn.ID())
		}
	}
}
