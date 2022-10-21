package commitment

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBranchData_MergeHexBranches2(t *testing.T) {
	row := make([]*Cell, 16)
	var bm uint16
	for i := 0; i < len(row); i++ {
		row[i] = new(Cell)
		row[i].hl = 32
		n, err := rand.Read(row[i].h[:])
		require.NoError(t, err)
		require.EqualValues(t, row[i].hl, n)

		th := rand.Intn(120)
		switch {
		case th > 80:
			n, err = rand.Read(row[i].apk[:])
			row[i].apl = n
		case th > 40 && th <= 80:
			n, err = rand.Read(row[i].spk[:])
			row[i].spl = n
		case th <= 40:
			n, err = rand.Read(row[i].extension[:th])
			row[i].extLen = n
			require.NoError(t, err)
			require.EqualValues(t, th, n)
		}
		bm |= uint16(i + 1)
	}

	enc, _, err := EncodeBranch(bm, bm, bm, func(i int, skip bool) (*Cell, error) {
		return row[i], nil
	})

	require.NoError(t, err)
	require.NotEmpty(t, enc)
	t.Logf("enc [%d] %x\n", len(enc), enc)

	//aix := make([]byte, 8192)
	bmg := NewHexBranchMerger(8192)
	//res, err := enc.MergeHexBranches(enc, nil)
	res, err := bmg.Merge(enc, enc)
	require.NoError(t, err)
	require.EqualValues(t, enc, res)

	tm, am, origins, err := res.DecodeCells()
	require.NoError(t, err)
	require.EqualValues(t, tm, am)
	require.EqualValues(t, bm, am)

	i := 0
	for _, c := range origins {
		if c == nil {
			continue
		}
		require.EqualValues(t, row[i].extLen, c.extLen)
		require.EqualValues(t, row[i].extension, c.extension)
		require.EqualValues(t, row[i].apl, c.apl)
		require.EqualValues(t, row[i].apk, c.apk)
		require.EqualValues(t, row[i].spl, c.spl)
		require.EqualValues(t, row[i].spk, c.spk)
		i++
	}
}
