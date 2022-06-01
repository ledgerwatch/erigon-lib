/*
   Copyright 2022 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package state

import "github.com/ledgerwatch/erigon-lib/kv"

// Reconstruction of the aggregator in another package, `aggregator`

type Aggregator struct {
	aggregationStep uint64
	accounts        *Domain
	storage         *Domain
	code            *Domain
	logAccounts     *InvertedIndex
	logTopics       *InvertedIndex
	tracesFrom      *InvertedIndex
	tracesTo        *InvertedIndex
}

func NewAggregator(
	dir string,
	aggregationStep uint64,
) (*Aggregator, error) {
	a := &Aggregator{
		aggregationStep: aggregationStep,
	}
	closeAgg := true
	defer func() {
		if closeAgg {
			if a.accounts != nil {
				a.accounts.Close()
			}
			if a.storage != nil {
				a.storage.Close()
			}
			if a.code != nil {
				a.code.Close()
			}
			if a.logAccounts != nil {
				a.logAccounts.Close()
			}
			if a.logTopics != nil {
				a.logTopics.Close()
			}
			if a.tracesFrom != nil {
				a.tracesFrom.Close()
			}
			if a.tracesTo != nil {
				a.tracesTo.Close()
			}
		}
	}()
	var err error
	if a.accounts, err = NewDomain(dir, aggregationStep, "accounts", kv.AccountKeys, kv.AccountVals, kv.AccountHistoryKeys, kv.AccountHistoryVals, kv.AccountSettings, kv.AccountIdx, 0 /* prefixLen */); err != nil {
		return nil, err
	}
	if a.storage, err = NewDomain(dir, aggregationStep, "storage", kv.StorageKeys, kv.StorageVals, kv.StorageHistoryKeys, kv.StorageHistoryVals, kv.StorageSettings, kv.StorageIdx, 20 /* prefixLen */); err != nil {
		return nil, err
	}
	if a.code, err = NewDomain(dir, aggregationStep, "code", kv.CodeKeys, kv.CodeVals, kv.CodeHistoryKeys, kv.CodeHistoryVals, kv.CodeSettings, kv.CodeIdx, 0 /* prefixLen */); err != nil {
		return nil, err
	}
	if a.logAccounts, err = NewInvertedIndex(dir, aggregationStep, "logaddrs", kv.LogAddressKeys, kv.LogAddressIdx); err != nil {
		return nil, err
	}
	if a.logTopics, err = NewInvertedIndex(dir, aggregationStep, "logtopics", kv.LogTopicsKeys, kv.LogTopicsIdx); err != nil {
		return nil, err
	}
	if a.tracesFrom, err = NewInvertedIndex(dir, aggregationStep, "tracesfrom", kv.TracesFromKeys, kv.TracesFromIdx); err != nil {
		return nil, err
	}
	if a.tracesTo, err = NewInvertedIndex(dir, aggregationStep, "tracesto", kv.TracesToKeys, kv.TracesToIdx); err != nil {
		return nil, err
	}
	closeAgg = false
	return a, nil
}

func (a *Aggregator) Close() {
	if a.accounts != nil {
		a.accounts.Close()
	}
	if a.storage != nil {
		a.storage.Close()
	}
	if a.code != nil {
		a.code.Close()
	}
	if a.logAccounts != nil {
		a.logAccounts.Close()
	}
	if a.logTopics != nil {
		a.logTopics.Close()
	}
	if a.tracesFrom != nil {
		a.tracesFrom.Close()
	}
	if a.tracesTo != nil {
		a.tracesTo.Close()
	}
}

func (a *Aggregator) SetTx(tx kv.RwTx) {
	a.accounts.SetTx(tx)
	a.storage.SetTx(tx)
	a.code.SetTx(tx)
	a.logAccounts.SetTx(tx)
	a.logTopics.SetTx(tx)
	a.tracesFrom.SetTx(tx)
	a.tracesTo.SetTx(tx)
}
