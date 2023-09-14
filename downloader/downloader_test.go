package downloader

import (
	"context"
	lg "github.com/anacrolix/log"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	downloadercfg2 "github.com/ledgerwatch/erigon-lib/downloader/downloadercfg"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestChangeInfoHashOfSameFile(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	cfg, err := downloadercfg2.New(dirs, "", lg.Info, 0, 0, 0, 0, 0, nil, "")
	require.NoError(t, err)
	d, err := New(context.Background(), cfg)
	require.NoError(t, err)
	err = d.AddInfoHashAsMagnetLink(context.Background(), snaptype.Hex2InfoHash("aa"), "a.seg")
	require.NoError(t, err)
	tt, ok := d.torrentClient.Torrent(snaptype.Hex2InfoHash("aa"))
	require.True(t, ok)
	require.Equal(t, "a.seg", tt.Name())

	// adding same file twice is ok
	err = d.AddInfoHashAsMagnetLink(context.Background(), snaptype.Hex2InfoHash("aa"), "a.seg")
	require.NoError(t, err)

	// adding same file with another infoHash - is ok, must be skipped
	// use-cases:
	//	- release of re-compressed version of same file,
	//	- ErigonV1.24 produced file X, then ErigonV1.25 released with new compression algorithm and produced X with anouther infoHash.
	//		ErigonV1.24 node must keep using existing file instead of downloading new one.
	err = d.AddInfoHashAsMagnetLink(context.Background(), snaptype.Hex2InfoHash("bb"), "a.seg")
	require.NoError(t, err)
	tt, ok = d.torrentClient.Torrent(snaptype.Hex2InfoHash("aa"))
	require.True(t, ok)
	require.Equal(t, "a.seg", tt.Name())
}
