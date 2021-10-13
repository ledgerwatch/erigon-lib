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

package compress

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

const maxMapSize = 0xFFFFFFFFFFFF

// mmap memory maps a DB's data file.
func mmap(f *os.File, size int) ([]byte, *[maxMapSize]byte, error) {
	// Map the data file to memory.
	b, err := unix.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, nil, err
	}

	// Advise the kernel that the mmap is accessed randomly.
	err = unix.Madvise(b, syscall.MADV_RANDOM)
	if err != nil && err != syscall.ENOSYS {
		// Ignore not implemented error in kernel because it still works.
		return nil, nil, fmt.Errorf("madvise: %s", err)
	}
	data := (*[maxMapSize]byte)(unsafe.Pointer(&b[0]))
	return b, data, nil
}

// munmap unmaps a DB's data file from memory.
func munmap(b []byte) error {
	// Ignore the unmap if we have no mapped data.
	if b == nil {
		return nil
	}
	// Unmap using the original byte slice.
	err := unix.Munmap(b)
	return err
}
