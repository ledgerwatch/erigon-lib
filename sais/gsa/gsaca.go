package gsa

/*
#include "gsacak.h"
#cgo CFLAGS: -DTERMINATOR=0 -DM64=1 -Dm64=1
*/
import "C"
import (
	"fmt"
	"unsafe"
)

// Implementation from https://github.com/felipelouza/gsufsort
// see also: https://almob.biomedcentral.com/track/pdf/10.1186/s13015-020-00177-y.pdf
// see also: https://almob.biomedcentral.com/track/pdf/10.1186/s13015-017-0117-9.pdf
func PrintArrays(str []byte, sa []uint, lcp []int, da []int32) {
	// remove terminator
	n := len(sa) - 1
	sa = sa[1:]
	lcp = lcp[1:]
	da = da[1:]

	fmt.Printf("i\t")
	fmt.Printf("sa\t")
	if lcp != nil {
		fmt.Printf("lcp\t")
	}
	if da != nil {
		fmt.Printf("gsa\t")
	}
	fmt.Printf("suffixes\t")
	fmt.Printf("\n")
	for i := 0; i < n; i++ {
		fmt.Printf("%d\t", i)
		fmt.Printf("n-%d=%d\t", sa[i], n-int(sa[i]))
		if lcp != nil {
			fmt.Printf("%d\t", lcp[i])
		}

		if da != nil { // gsa
			value := sa[i]
			if da[i] != 0 {
				value = sa[i] - sa[da[i]-1] - 1
			}
			fmt.Printf("(%d %d)\t", da[i], value)
		}
		//bwt
		if sa[i] == 0 {
			fmt.Printf("$\t")
		} else {
			c := str[sa[i]-1] - 1
			if c == 0 {
				c = '$'
			}
			fmt.Printf("%c\t", c)
		}

		//	char c = (SA[i])? T[SA[i]-1]-1:terminal;
		//	if(c==0) c = '$';
		//	printf("%c\t",c);

		//suffixes
		for j := sa[i]; int(j) < n; j++ {
			if str[j] == 1 {
				fmt.Printf("$")
				break
			} else if str[j] == 0 {
				fmt.Printf("#")
			} else {
				fmt.Printf("%c", str[j]-1)
			}
		}
		fmt.Printf("\n")
	}
}

//nolint
// SA2GSA - example func to convert SA+DA to GSA
func SA2GSA(sa []uint, da []int32) []uint {
	// remove terminator
	sa = sa[1:]
	da = da[1:]
	n := len(sa) - 1

	gsa := make([]uint, n)
	copy(gsa, sa)

	for i := 0; i < n; i++ {
		if da[i] != 0 {
			pos := sa[da[i]-1]
			//posAfter := sa[da[i]-1]
			//length := posAfter - pos
			//
			gsa[i] = sa[i] - pos - 1
		}
	}
	return gsa
}

func PrintRepeats(str []byte, sa []uint, lcp []int, da []int32) {
	sa = sa[1:]
	lcp = lcp[1:]
	da = da[1:]
	n := len(sa) - 1
	fmt.Printf("== Repeatst ==\n")
	var stack []int
	top := func() int { return stack[len(stack)-1] }
	pop := func() { stack = stack[:len(stack)-1] }
	push := func(i int) { stack = append(stack, i) }
	count := make([]int, n+1)
	count[sa[n-1]] = 1

	stack = stack[:0]
	var j int
	for i := 0; i < n-1; i++ {
		//fmt.Printf("alex foud: %d-%d\n", n, j)
		for j = i; lcp[j+1] >= lcp[j]; j++ {
		}
		fmt.Printf("alex foud: %d-%d\n", i, j)
	}
	panic(1)
	for k := 11; k >= 6; k-- {

		for j := sa[k]; int(j) < n; j++ {
			if str[j] == 1 {
				fmt.Printf("$")
				break
			} else if str[j] == 0 {
				fmt.Printf("#")
			} else {
				fmt.Printf("%c", str[j]-1)
			}
		}
		fmt.Printf("\n")

		posAfter := sa[da[k]]
		l := posAfter - sa[k]

		//fmt.Printf("a: %d\n", k)
		fmt.Printf("a: %d, %d, %d, %d\n", lcp[k], sa[k], n-int(sa[k]), stack)
		for len(stack) > 0 {
			//fmt.Printf("stop?: %d, %d\n", lcp[top()], lcp[k])
			if lcp[top()] < lcp[k] {
				break
			}
			pop()
		}
		gsa := sa[k]
		if da[k] != 0 {
			gsa = sa[k] - sa[da[k]-1] - 1
		}
		_, _ = l, gsa
		if int(lcp[k]) == n-int(sa[k]) {
			if len(stack) == 0 {
				count[sa[k]] = n - k
			} else {
				count[sa[k]] = top() - k
			}
		} else {
			count[sa[k]] = 1
		}
		fmt.Printf("count: %d, %d,%d, %d\n", gsa, int(gsa)-k, n-int(sa[k]), count)

		push(k)
	}
	fmt.Printf("count: %d\n", count)
	return
	var repeats int
	for i := 0; i < n; i++ {
		repeats++
		if da[i] < da[i+1] { // same suffix
			continue
		}

		// new suffix
		fmt.Printf(" repeats: %d, %d\t", repeats, lcp[i])
		for j := sa[i]; int(j) < n; j++ {
			if str[j] == 1 {
				//fmt.Printf("$")
				break
			} else if str[j] == 0 {
				fmt.Printf("#")
			} else {
				fmt.Printf("%c", str[j]-1)
			}
		}
		fmt.Printf("\n")

		repeats = 0
	}
}

func GSA(data []byte, sa []uint, lcp []int, da []int32) error {
	tPtr := unsafe.Pointer(&data[0]) // source "text"
	var lcpPtr, saPtr, daPtr unsafe.Pointer
	if sa != nil {
		saPtr = unsafe.Pointer(&sa[0])
	}
	if lcp != nil {
		lcpPtr = unsafe.Pointer(&lcp[0])
	}
	if da != nil {
		daPtr = unsafe.Pointer(&da[0])
	}
	depth := C.gsacak(
		(*C.uchar)(tPtr),
		(*C.uint_t)(saPtr),
		(*C.int_t)(lcpPtr),
		(*C.int_da)(daPtr),
		C.uint_t(len(data)),
	)
	_ = depth
	return nil
}

func ConcatAll(R [][]byte) (str []byte, n int) {
	for i := 0; i < len(R); i++ {
		n += len(R[i]) + 1
	}

	n++ //add 0 at the end
	str = make([]byte, n)
	var l int

	for i := 0; i < len(R); i++ {
		m := len(R[i])
		for j := 0; j < m; j++ {
			if R[i][j] < 255 && R[i][j] > 1 {
				str[l] = R[i][j] + 1
				l++
			}
		}
		if m > 0 {
			if str[l-1] > 1 {
				str[l] = 1
				l++
			} //add 1 as separator (ignores empty entries)
		}
	}
	str[l] = 0
	l++
	n = l
	return str, n
}
