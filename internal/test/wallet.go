package test

import (
	"errors"
	"sync"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/siad/modules"
)

type (
	SiacoinElement struct {
		ID     types.SiacoinOutputID
		Output types.SiacoinOutput
	}
	Wallet struct {
		privateKey types.PrivateKey

		mu     sync.Mutex
		utxos  map[types.SiacoinOutputID]SiacoinElement
		locked map[types.SiacoinOutputID]bool
	}
)

func (w *Wallet) ProcessConsensusChange(cc modules.ConsensusChange) {
	w.mu.Lock()
	defer w.mu.Unlock()

	addr := w.privateKey.PublicKey().StandardAddress()
	for _, sco := range cc.SiacoinOutputDiffs {
		if types.Address(sco.SiacoinOutput.UnlockHash) != addr {
			continue
		}

		scoID := types.SiacoinOutputID(sco.ID)
		var output types.SiacoinOutput
		convertToCore(sco.SiacoinOutput, &output)

		switch sco.Direction {
		case modules.DiffApply:
			w.utxos[scoID] = SiacoinElement{
				ID:     scoID,
				Output: output,
			}
		case modules.DiffRevert:
			delete(w.utxos, scoID)
			delete(w.locked, scoID)
		}
	}
}

func (w *Wallet) FundTransaction(txn *types.Transaction, amount types.Currency) (toSign []types.Hash256, release func(), err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	uc := w.privateKey.PublicKey().StandardUnlockConditions()
	var added types.Currency
	for _, sco := range w.utxos {
		if w.locked[sco.ID] {
			continue
		} else if added.Cmp(amount) >= 0 {
			break
		}

		txn.SiacoinInputs = append(txn.SiacoinInputs, types.SiacoinInput{
			ParentID:         sco.ID,
			UnlockConditions: uc,
		})
		added = added.Add(sco.Output.Value)
		toSign = append(toSign, types.Hash256(sco.ID))
	}

	if added.Cmp(amount) < 0 {
		return nil, func() {}, errors.New("insufficient funds")
	} else if added.Cmp(amount) > 0 {
		change := added.Sub(amount)
		txn.SiacoinOutputs = append(txn.SiacoinOutputs, types.SiacoinOutput{
			Value:   change,
			Address: uc.UnlockHash(),
		})
	}

	for _, id := range toSign {
		w.locked[types.SiacoinOutputID(id)] = true
	}

	return toSign, func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		for _, id := range toSign {
			delete(w.locked, types.SiacoinOutputID(id))
		}
	}, nil
}

func (w *Wallet) Address() types.Address {
	return w.privateKey.PublicKey().StandardAddress()
}

func (w *Wallet) Balance() (spendable, confirmed types.Currency) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, sco := range w.utxos {
		confirmed = confirmed.Add(sco.Output.Value)
		if !w.locked[sco.ID] {
			spendable = spendable.Add(sco.Output.Value)
		}
	}
	return
}

func (w *Wallet) Sign(txn *types.Transaction, cs consensus.State, toSign []types.Hash256, cf types.CoveredFields) error {
	for _, id := range toSign {
		var h types.Hash256
		if cf.WholeTransaction {
			h = cs.WholeSigHash(*txn, id, 0, 0, cf.Signatures)
		} else {
			h = cs.PartialSigHash(*txn, cf)
		}
		sig := w.privateKey.SignHash(h)
		txn.Signatures = append(txn.Signatures, types.TransactionSignature{
			ParentID:       id,
			CoveredFields:  cf,
			PublicKeyIndex: 0,
			Signature:      sig[:],
		})
	}
	return nil
}

func NewWallet() *Wallet {
	return &Wallet{
		privateKey: types.GeneratePrivateKey(),
		locked:     make(map[types.SiacoinOutputID]bool),
		utxos:      make(map[types.SiacoinOutputID]SiacoinElement),
	}
}
