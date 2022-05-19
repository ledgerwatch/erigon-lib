package common

import (
	"runtime"
)

var DoMemstat = true

func ReadMemStats(m *runtime.MemStats) {
	if DoMemstat {
		runtime.ReadMemStats(m)
	}
}
