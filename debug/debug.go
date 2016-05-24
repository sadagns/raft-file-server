package debug

import (
	"fmt"
)

const (
	TEST = false
	SERVER = false
	RN = false
	RSM = false
	PRINT = false
)

func Print(msg ...interface{}) {
	if PRINT {
		fmt.Println(msg)
	}
}

func Test(msg ...interface{}) {
	if TEST {
		fmt.Println("test:", msg)
	}
}

func Server(msg ...interface{}) {
	if SERVER {
		fmt.Println("server:", msg)
	}
}

func Rn(msg ...interface{}) {
	if RN {
		fmt.Println("rn:", msg)
	}
}

func Rsm(msg ...interface{}) {
	if RSM {
		fmt.Println("rsm:", msg)
	}
}
