package main

import (
	"encoding/json"
	"log"
	"os"

	"go.sia.tech/core/types"
)

type (
	jsonSiacoinOutput struct {
		UnlockHash types.Address         `json:"unlock_hash"`
		OutputID   types.SiacoinOutputID `json:"output_id"`
		Value      types.Currency        `json:"value"`
	}

	jsonStorageContract struct {
		ValidProofOutputs  []jsonSiacoinOutput `json:"valid_proof_outputs"`
		MissedProofOutputs []jsonSiacoinOutput `json:"missed_proof_outputs"`
	}

	jsonTransaction struct {
		Fees             types.Currency        `json:"fees"`
		SiacoinInputs    []jsonSiacoinOutput   `json:"siacoin_inputs"`
		SiacoinOutputs   []jsonSiacoinOutput   `json:"siacoin_outputs"`
		StorageContracts []jsonStorageContract `json:"storage_contracts"`
	}

	siacoinElement struct {
		ID      types.SiacoinOutputID
		Value   types.Currency
		Address types.Address
	}
)

func sum(eles []siacoinElement) types.Currency {
	var sum types.Currency
	for _, ele := range eles {
		sum = sum.Add(ele.Value)
	}
	return sum
}

func estimateHostFunds(inputs, outputs []siacoinElement, renterTarget, hostTarget types.Currency) (types.Currency, bool) {
	for i := range inputs {
		renterInput, hostInput := sum(inputs[:i]), sum(inputs[i:])

		for j := len(outputs); j >= 0; j-- {
			renterOutput, hostOutput := sum(outputs[:j]), sum(outputs[j:])

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

func main() {
	f, err := os.Open("test.json")
	if err != nil {
		log.Fatalln(err)
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	var txn jsonTransaction
	if err := dec.Decode(&txn); err != nil {
		log.Fatalln(err)
	}

	var inputs, outputs []siacoinElement
	for _, inp := range txn.SiacoinInputs {
		inputs = append(inputs, siacoinElement{
			ID:      inp.OutputID,
			Value:   inp.Value,
			Address: inp.UnlockHash,
		})
	}
	for _, out := range txn.SiacoinOutputs {
		outputs = append(outputs, siacoinElement{
			ID:      out.OutputID,
			Value:   out.Value,
			Address: out.UnlockHash,
		})
	}

	renterTarget := txn.Fees.Add(txn.StorageContracts[0].ValidProofOutputs[0].Value)
	hostTarget := txn.StorageContracts[0].MissedProofOutputs[1].Value
	hostFunds, ok := estimateHostFunds(inputs, outputs, renterTarget, hostTarget)
	if !ok {
		log.Println("no solution")
	}
	log.Println("locked collateral", hostFunds)
}
