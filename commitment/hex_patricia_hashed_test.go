/*
   Copyright 2021 Erigon contributors

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

package commitment

import "testing"

// In memory commitment and state to use with the tests
type MockState struct {
}

func (ms MockState) branchFn(prefix []byte, row []Cell) error {
	return nil
}

func (ms MockState) accountFn(plainKey []byte, account *AccountDecorator) error {
	return nil
}

func (ms MockState) storageFn(plainKey []byte, storage []byte) error {
	return nil
}

func TestEmptyState(t *testing.T) {
	var ms MockState
	hph := &HexPatriciaHashed{
		branchFn:  ms.branchFn,
		accountFn: ms.accountFn,
		storageFn: ms.storageFn,
	}
	if err := hph.unfoldCell(&hph.root, 0); err != nil {
		t.Error(err)
	}
}
