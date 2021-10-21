package debug

import (
	"fmt"
	"runtime/debug"
	"strings"
)

var panicReplacer = strings.NewReplacer("\n", " ", "\t", "", "\r", "")

// Recover - does save panic to datadir/crashreports, bud doesn't log to logger and doesn't stop the process
// it returns recovered panic as error in format friendly for our logger
// common pattern of use - assign to named output param:
//  func A() (err error) {
//	    defer func() { err = debug.ReportPanicAndRecover() }() // avoid crash because Erigon's core does many things
//  }
func Recover(err error) error {
	panicResult := recover()
	if panicResult == nil {
		return err
	}

	stack := string(debug.Stack())
	switch typed := panicResult.(type) {
	case error:
		err = fmt.Errorf("%w, trace: %s", typed, panicReplacer.Replace(stack))
	default:
		err = fmt.Errorf("%+v, trace: %s", typed, panicReplacer.Replace(stack))
	}
	return err
}
