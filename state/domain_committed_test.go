package state

//func collateAndMergeCommitted(t *testing.T, db kv.RwDB, d *DomainCommitted, txs uint64) {
//	t.Helper()
//	var tx kv.RwTx
//	defer func() {
//		if tx != nil {
//			tx.Rollback()
//		}
//	}()
//	// Leave the last 2 aggregation steps un-collated
//	for step := uint64(0); step < txs/d.aggregationStep-1; step++ {
//		func() {
//			roTx, err := db.BeginRo(context.Background())
//			require.NoError(t, err)
//			defer roTx.Rollback()
//			c, err := d.collate(step, step*d.aggregationStep, (step+1)*d.aggregationStep, roTx)
//			require.NoError(t, err)
//			roTx.Rollback()
//			sf, err := d.buildFiles(step, c)
//			require.NoError(t, err)
//			d.integrateFiles(sf, step*d.aggregationStep, (step+1)*d.aggregationStep)
//			tx, err = db.BeginRw(context.Background())
//			require.NoError(t, err)
//			d.SetTx(tx)
//			err = d.prune(step, step*d.aggregationStep, (step+1)*d.aggregationStep, math.MaxUint64)
//			require.NoError(t, err)
//			err = tx.Commit()
//			require.NoError(t, err)
//			tx = nil
//			var r DomainRanges
//			maxEndTxNum := d.endTxNumMinimax()
//			maxSpan := uint64(16 * 16)
//			for r = d.findMergeRange(maxEndTxNum, maxSpan); r.any(); r = d.findMergeRange(maxEndTxNum, maxSpan) {
//				valuesOuts, indexOuts, historyOuts, _ := d.staticFilesInRange(r)
//				valuesIn, indexIn, historyIn, err := d.mergeFiles(valuesOuts, indexOuts, historyOuts, r, maxSpan)
//				require.NoError(t, err)
//				d.integrateMergedFiles(valuesOuts, indexOuts, historyOuts, valuesIn, indexIn, historyIn)
//				err = d.deleteFiles(valuesOuts, indexOuts, historyOuts)
//				require.NoError(t, err)
//			}
//		}()
//	}
//}
//func TestDomainCommitted_ReplaceKeysOnMerge(t *testing.T) {
//	keysCount, txCount := uint64(16), uint64(64)
//
//	path, db, d := testDbAndDomain(t, 0 /* prefixLen */)
//	defer db.Close()
//	defer d.Close()
//	defer os.Remove(path)
//
//	cd := NewCommittedDomain(d, CommitmentModeUpdate)
//	cd.mergeFiles()
//
//	tx, err := db.BeginRw(context.Background())
//	require.NoError(t, err)
//	defer func() {
//		if tx != nil {
//			tx.Rollback()
//		}
//	}()
//	d.SetTx(tx)
//	// keys are encodings of numbers 1..31
//	// each key changes value on every txNum which is multiple of the key
//	data := make(map[string][]uint64)
//
//	for txNum := uint64(1); txNum <= txCount; txNum++ {
//		d.SetTxNum(txNum)
//		for keyNum := uint64(1); keyNum <= keysCount; keyNum++ {
//			if keyNum == txNum%d.aggregationStep {
//				continue
//			}
//			var k [8]byte
//			var v [8]byte
//			binary.BigEndian.PutUint64(k[:], keyNum)
//			binary.BigEndian.PutUint64(v[:], txNum)
//			err = d.Put(k[:], nil, v[:])
//			require.NoError(t, err)
//
//			list, ok := data[fmt.Sprintf("%d", keyNum)]
//			if !ok {
//				data[fmt.Sprintf("%d", keyNum)] = make([]uint64, 0)
//			}
//			data[fmt.Sprintf("%d", keyNum)] = append(list, txNum)
//		}
//		if txNum%d.aggregationStep == 0 {
//			step := txNum/d.aggregationStep - 1
//			if step == 0 {
//				continue
//			}
//			step--
//			collateAndMergeOnce(t, d, step)
//
//			err = tx.Commit()
//			require.NoError(t, err)
//			tx, err = db.BeginRw(context.Background())
//			require.NoError(t, err)
//			d.SetTx(tx)
//		}
//	}
//	err = tx.Commit()
//	require.NoError(t, err)
//	tx = nil
//
//	roTx, err := db.BeginRo(context.Background())
//	require.NoError(t, err)
//	defer roTx.Rollback()
//
//	// Check the history
//	dc := d.MakeContext()
//	for txNum := uint64(1); txNum <= txCount; txNum++ {
//		for keyNum := uint64(1); keyNum <= keysCount; keyNum++ {
//			valNum := txNum
//			var k [8]byte
//			var v [8]byte
//			label := fmt.Sprintf("txNum=%d, keyNum=%d\n", txNum, keyNum)
//			binary.BigEndian.PutUint64(k[:], keyNum)
//			binary.BigEndian.PutUint64(v[:], valNum)
//
//			val, err := dc.GetBeforeTxNum(k[:], txNum+1, roTx)
//			if keyNum == txNum%d.aggregationStep {
//				if txNum > 1 {
//					binary.BigEndian.PutUint64(v[:], txNum-1)
//					require.EqualValues(t, v[:], val)
//					continue
//				} else {
//					require.Nil(t, val, label)
//					continue
//				}
//			}
//			require.NoError(t, err, label)
//			require.EqualValues(t, v[:], val, label)
//		}
//	}
//
//	var v [8]byte
//	binary.BigEndian.PutUint64(v[:], txCount)
//
//	for keyNum := uint64(1); keyNum <= keysCount; keyNum++ {
//		var k [8]byte
//		label := fmt.Sprintf("txNum=%d, keyNum=%d\n", txCount, keyNum)
//		binary.BigEndian.PutUint64(k[:], keyNum)
//
//		storedV, err := dc.Get(k[:], nil, roTx)
//		require.NoError(t, err, label)
//		require.EqualValues(t, v[:], storedV, label)
//	}
//}
