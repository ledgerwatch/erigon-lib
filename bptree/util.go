package bptree

const BufferSize uint = 4096

func ensure(condition bool, message string) {
	if !condition {
		panic(message)
	}
}
