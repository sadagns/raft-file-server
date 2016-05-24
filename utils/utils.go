package utils

import (
	"time"
	"math/rand"	
	"os"
	"os/exec"
	"fmt"	
)

// Check for error condition. 
func Assert(val bool, msg ...interface{}) {
	if !val {
		fmt.Println("utils: assert:", msg)
		panic("Assertion failed")
	}
}

// Return a random value between two int64
func RandRange(min, max int64) int64 {	
	rand.Seed(time.Now().Unix())
	return int64(rand.Intn(int(max - min))) + min
}

// Return minimum of two int64
func Min(a, b int64) int64 {
	if a <= b {	
		return a
	} 
	return b
}

// Return maximum of two int64
func Max(a, b int64) int64 {
	if a >= b {	
		return a
	} 
	return b
}

// Run the input command in bash shell and return without waiting for completion
func RunCmd(proc string) (err error) {
	var (
		cmd *exec.Cmd
	)

	cmd = exec.Command("sh", "-c", proc)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	return err
}