package main

import (
	"fmt"

	"github.com/ledgerwatch/erigon-lib/aggregator"
)

func main() {
	if err := aggregator.Reproduce(); err != nil {
		fmt.Printf("%v\n", err)
	}
}
