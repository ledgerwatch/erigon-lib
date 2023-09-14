package downloader

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestChangeInfoHashOfSameFile(t *testing.T) {
	d, err := New(context.Background(), nil)
	require.NoError(t, err)
	_ = d

}
