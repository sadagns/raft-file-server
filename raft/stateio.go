package raft

import (
	"bufio"
	"os"
	"fmt"
	"strconv"
	"errors"
)

// State store API
type stateIo interface {
	readState() (StateStoreAction, error)
	writeState(StateStoreAction) error
	close() error
}

// State store structure
type StateStore struct {
	fileName string
}

// Create a new stateio object
func newStateIo(fileName string) (stateIo, error) {
	var (
		ss StateStore
		file *os.File
		err error
	)

	ss.fileName = fileName
	file, err = os.Open(ss.fileName)
	if os.IsNotExist(err) {
		file, err = os.Create(ss.fileName)
	}
	if err == nil {
		file.Close()
	}
	return &ss, err
}

// Return state variables from the disk file
func (ss *StateStore) readState() (StateStoreAction, error) {
	var (
		file *os.File
		scanner *bufio.Scanner
		lines []string
		stateStore StateStoreAction
		err error
	)

	file, err = os.Open(ss.fileName)
	if err != nil {
		return stateStore, err
	}
	defer file.Close()
	scanner = bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if scanner.Err() != nil {
		return stateStore, scanner.Err()
	}
	if len(lines) < 3 {
		return stateStore, errors.New("ERR_NOT_SET")
	}
	stateStore.CurrTerm, _ = strconv.Atoi(lines[0])
	stateStore.VotedFor, _ = strconv.Atoi(lines[1])
	stateStore.LastRepIndex, _ = strconv.ParseInt(lines[2], 10, 64)
	return stateStore, nil
}

// Write state variables into disk file
func (ss *StateStore) writeState(stateStore StateStoreAction) error {
	var (
		err error
		file *os.File
		writer *bufio.Writer
	)

	file, err = os.Create(ss.fileName)
	if err != nil {
		return err
	}
	defer file.Close()
	writer = bufio.NewWriter(file)
	fmt.Fprintln(writer, stateStore.CurrTerm)
	fmt.Fprintln(writer, stateStore.VotedFor)
	fmt.Fprintln(writer, stateStore.LastRepIndex)
	return writer.Flush()
}

// Remove disk file
func (ss *StateStore) close() error {
	return os.Remove(ss.fileName)
}