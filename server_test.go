package main

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"bytes"
	"time"
	"os"
	"os/exec"
	"sync"
	"github.com/sadagopanns/cs733/assignment4/utils"
	"github.com/sadagopanns/cs733/assignment4/debug"
)

const (
	LDR_ELECT_TIME = 2000
)

var (
	procs map[int]*exec.Cmd
)

func initTest() (err error) {
	err = utils.RunCmd("./clean.sh")
	if err != nil {
		return err
	}
	err = utils.RunCmd("go install server.go")
	return err
}

func exitTest() (err error) {
	err = utils.RunCmd("./clean.sh")
	return err
}

func begServer(id int) (err error) {
	procs[id] = exec.Command("sh", "-c", "server " + strconv.Itoa(id))
	procs[id].Stdout = os.Stdout
	procs[id].Stderr = os.Stderr
	err = procs[id].Start()
	return err
}

func startCluster() (err error) {
	var (
		i int
	)

	procs = make(map[int]*exec.Cmd)
	for i = 1; i <= 5; i++ {
		err = begServer(i)
		if err != nil {
			return err
		}
	}
	return nil
}

func killServer(id int) (err error) {
	err = utils.RunCmd("./kill.sh server." + strconv.Itoa(id))
	return err
}

func killCluster() (err error) {
	var (
		i int
	)

	for i = 1; i <= 5; i++ {
		err = killServer(i)
		if err != nil {
			return err
		}
	}
	return nil
}

func TestDebug(t *testing.T) {
	debug.Test("")
}

func TestBasic(t *testing.T) {
	var (
		cl *Client
		m *Msg
		err error
		txt string
	)

	err = initTest()
	checkErr(t, err, "basic: inittest")

	err = startCluster()
	checkErr(t, err, "basic: startcluster")
	time.Sleep(5 * time.Second)

	cl = mkClient(t, 5001)
	defer cl.close()

	m, err = cl.read("cs733net")
	expect(t, m, &Msg{Kind: 'F'}, "file not found", err)

	txt = "foo"
	m, err = cl.write("cs733net", txt, 0)
	expect(t, m, &Msg{Kind: 'O'}, "write success", err)

	err = killCluster()
	checkErr(t, err, "basic: killcluster")
	err = exitTest()
	checkErr(t, err, "basic: exittest")	
}	

////////////////////////////////////////////////////////////////////////////////////////////////////
// 
// CS733 Assignment-1 Test cases
// 
////////////////////////////////////////////////////////////////////////////////////////////////////

func TestRPC_BasicSequential(t *testing.T) {
	var (
		cl *Client
		m *Msg
		err error
		data string
		version1 int
	)

	err = initTest()
	checkErr(t, err, "rpcbasicsequential: inittest")

	err = startCluster()
	checkErr(t, err, "rpcbasicsequential: startcluster")
	time.Sleep(5 * time.Second)

	cl = mkClient(t, 5001)
	defer cl.close()

	// Read non-existent file cs733net
	m, err = cl.read("cs733net")
	expect(t, m, &Msg{Kind: 'F'}, "file not found", err)

	// Delete non-existent file cs733net
	m, err = cl.delete("cs733net")
	expect(t, m, &Msg{Kind: 'F'}, "file not found", err)

	// Write file cs733net
	data = "Cloud fun"
	m, err = cl.write("cs733net", data, 0)
	expect(t, m, &Msg{Kind: 'O'}, "write success", err)

	// Expect to read it back
	m, err = cl.read("cs733net")
	expect(t, m, &Msg{Kind: 'C', Contents: []byte(data)}, "read my write", err)

	// CAS in new value
	version1 = m.Version
	data2 := "Cloud fun 2"
	// Cas new value
	m, err = cl.cas("cs733net", version1, data2, 0)
	expect(t, m, &Msg{Kind: 'O'}, "cas success", err)

	// Expect to read it back
	m, err = cl.read("cs733net")
	expect(t, m, &Msg{Kind: 'C', Contents: []byte(data2)}, "read my cas", err)

	// Expect Cas to fail with old version
	m, err = cl.cas("cs733net", version1, data, 0)
	expect(t, m, &Msg{Kind: 'V'}, "cas version mismatch", err)

	// Expect a failed cas to not have succeeded. Read should return data2.
	m, err = cl.read("cs733net")
	expect(t, m, &Msg{Kind: 'C', Contents: []byte(data2)}, "failed cas to not have succeeded", err)

	// delete
	m, err = cl.delete("cs733net")
	expect(t, m, &Msg{Kind: 'O'}, "delete success", err)

	// Expect to not find the file
	m, err = cl.read("cs733net")
	expect(t, m, &Msg{Kind: 'F'}, "file not found", err)

	err = exitTest()
	checkErr(t, err, "rpcbasicsequential: exittest")		
}

func TestRPC_Binary(t *testing.T) {
	var (
		cl *Client
		data string
		m *Msg
		err error
	)

	err = initTest()
	checkErr(t, err, "rpcbinary: inittest")

	err = startCluster()
	checkErr(t, err, "rpcbinary: startcluster")
	time.Sleep(5 * time.Second)

	cl = mkClient(t, 5001)
	defer cl.close()

	// Write binary contents
	data = "\x00\x01\r\n\x03" // some non-ascii, some crlf chars
	m, err = cl.write("binfile", data, 0)
	expect(t, m, &Msg{Kind: 'O'}, "write success", err)

	// Expect to read it back
	m, err = cl.read("binfile")
	expect(t, m, &Msg{Kind: 'C', Contents: []byte(data)}, "read my write", err)

	err = exitTest()
	checkErr(t, err, "rpcbinary: exittest")		
}

func TestRPC_Chunks(t *testing.T) {
	var (
		cl *Client
		m *Msg
		err error
		res bool
	)

	err = initTest()
	checkErr(t, err, "rpcchunks: inittest")

	err = startCluster()
	checkErr(t, err, "rpcchunks: startcluster")
	time.Sleep(5 * time.Second)

	// Should be able to accept a few bytes at a time
	cl = mkClient(t, 5001)
	defer cl.close()

	snd := func(chunk string) {
		if err == nil {
			err = cl.send(chunk)
		}
	}


	for {

		// Send the command "write teststream 10\r\nabcdefghij\r\n" in multiple chunks
		// Nagle's algorithm is disabled on a write, so the server should get these in separate TCP packets.
		snd("wr")
		time.Sleep(10 * time.Millisecond)
		snd("ite test")
		time.Sleep(10 * time.Millisecond)
		snd("stream 1")
		time.Sleep(10 * time.Millisecond)
		snd("0\r\nabcdefghij\r")
		time.Sleep(10 * time.Millisecond)
		snd("\n")

		m, err = cl.rcv()

		if err == nil {
			res, err = cl.checkRT(m)
			if res && err == nil {
				continue
			}
		}
		break	
	}

	expect(t, m, &Msg{Kind: 'O'}, "writing in chunks should work", err)

	err = exitTest()
	checkErr(t, err, "rpchunks: exittest")	
}

func TestRPC_Batch(t *testing.T) {
	var (
		cl *Client
		err error
		cmds string
		m1 *Msg
		m2 *Msg
		m3 *Msg
		res bool
	)

	err = initTest()
	checkErr(t, err, "rpcbatch: inittest")

	err = startCluster()
	checkErr(t, err, "rpcbatch: startcluster")
	time.Sleep(5 * time.Second)

	// Send multiple commands in one batch, expect multiple responses
	cl = mkClient(t, 5001)
	defer cl.close()
	
	cmds = "write batch1 3\r\nabc\r\n" +
		"write batch2 4\r\ndefg\r\n" +
		"read batch1\r\n"

	for {
		cl.send(cmds)
		
		m1, err = cl.rcv()
		if err != nil {
			break
		}

		m2, err = cl.rcv()
		if err != nil {
			break
		}


		m3, err = cl.rcv()
		if err != nil {
			break
		}
		
		res, err = cl.checkRT(m1)
		if res && err == nil {
			continue
		} else if err != nil {
			break
		}

		res, err = cl.checkRT(m2)
		if res && err == nil {
			continue
		} else if err != nil {
			break
		}		

		res, err = cl.checkRT(m3)
		if res && err == nil {
			continue
		} else {
			break
		}

	}

	checkErr(t, err, "rpcbatch: error")

	if !((m1.Kind == 'O' || m1.Kind == 'F' || m1.Kind == 'C') && (m2.Kind == 'O' || m2.Kind == 'F' || m2.Kind == 'C') && (m3.Kind == 'O' || m3.Kind == 'F' || m3.Kind == 'C')) {
		t.Fatal("rpcbatch: error: expecting only from 'O' or 'F' or 'C'")
	}

	err = exitTest()
	checkErr(t, err, "rpcbatch: exittest")	
}

func TestRPC_BasicTimer(t *testing.T) {
	var (
		cl *Client
		m *Msg
		err error
		str string
	)


	err = initTest()
	checkErr(t, err, "rpcbasictimer: inittest")

	err = startCluster()
	checkErr(t, err, "rpcbasictimer: startcluster")
	time.Sleep(5 * time.Second)

	cl = mkClient(t, 5001)
	defer cl.close()

	// Write file cs733, with expiry time of 2 seconds
	str = "Cloud fun"
	m, err = cl.write("cs733", str, 2)
	expect(t, m, &Msg{Kind: 'O'}, "write success", err)

	// Expect to read it back immediately.
	m, err = cl.read("cs733")
	expect(t, m, &Msg{Kind: 'C', Contents: []byte(str)}, "read my cas", err)

	time.Sleep(3 * time.Second)

	// Expect to not find the file after expiry
	m, err = cl.read("cs733")
	expect(t, m, &Msg{Kind: 'F'}, "file not found", err)

	// Recreate the file with expiry time of 1 second
	m, err = cl.write("cs733", str, 1)
	expect(t, m, &Msg{Kind: 'O'}, "file recreated", err)

	// Overwrite the file with expiry time of 4. This should be the new time.
	m, err = cl.write("cs733", str, 3)
	expect(t, m, &Msg{Kind: 'O'}, "file overwriten with exptime=4", err)

	// The last expiry time was 3 seconds. We should expect the file to still be around 2 seconds later
	time.Sleep(2 * time.Second)

	// Expect the file to not have expired.
	m, err = cl.read("cs733")
	expect(t, m, &Msg{Kind: 'C', Contents: []byte(str)}, "file to not expire until 4 sec", err)

	time.Sleep(3 * time.Second)
	// 5 seconds since the last write. Expect the file to have expired
	m, err = cl.read("cs733")
	expect(t, m, &Msg{Kind: 'F'}, "file not found after 4 sec", err)

	// Create the file with an expiry time of 1 sec. We're going to delete it
	// then immediately create it. The new file better not get deleted. 
	m, err = cl.write("cs733", str, 1)
	expect(t, m, &Msg{Kind: 'O'}, "file created for delete", err)

	m, err = cl.delete("cs733")
	expect(t, m, &Msg{Kind: 'O'}, "deleted ok", err)

	m, err = cl.write("cs733", str, 0) // No expiry
	expect(t, m, &Msg{Kind: 'O'}, "file recreated", err)

	time.Sleep(1100 * time.Millisecond) // A little more than 1 sec
	m, err = cl.read("cs733")
	expect(t, m, &Msg{Kind: 'C'}, "file should not be deleted", err)

	err = exitTest()
	checkErr(t, err, "rpcbasictimer: exittest")	
}

// nclients write to the same file. At the end the file should be
// any one clients' last write
func TestRPC_ConcurrentWrites(t *testing.T) {
	var (
		nclients int
		niters int
		clients []*Client
		i int
		cl *Client
		errCh chan error
		err error
		ch chan *Msg
		sem sync.WaitGroup // Used as a semaphore to coordinate goroutines to begin concurrently
		m *Msg
	)

	err = initTest()
	checkErr(t, err, "rpc_concurrent_writes: inittest")

	err = startCluster()
	checkErr(t, err, "rpc_concurrent_writes: startcluster")
	time.Sleep(5 * time.Second)

	nclients = 5
	niters = 5
	clients = make([]*Client, nclients)
	for i = 0; i < nclients; i++ {
		cl = mkClient(t, 5001)
		if cl == nil {
			t.Fatalf("Unable to create client #%d", i)
		}
		defer cl.close()
		clients[i] = cl
	}

	errCh = make(chan error, nclients)
	sem.Add(1)
	ch = make(chan *Msg, nclients * niters) // channel for all replies
	for i := 0; i < nclients; i++ {
		go func(i int, cl *Client) {
			sem.Wait()
			for j := 0; j < niters; j++ {
				str := fmt.Sprintf("cl %d %d", i, j)
				m, err := cl.write("concWrite", str, 0)
				if err != nil {
					errCh <- err
					break
				} else {
					ch <- m
				}
				debug.Test("rpc_concurrent_writes:", i, j)
			}
		}(i, clients[i])
	}
	time.Sleep(100 * time.Millisecond) // give goroutines a chance
	sem.Done()                         // Go!

	// There should be no errors
	for i = 0; i < nclients * niters; i++ {
		select {
		case m = <-ch:
			if m.Kind != 'O' {
				t.Fatalf("Concurrent write failed with kind=%c", m.Kind)
			}
		case err := <- errCh:
			t.Fatal(err)
		}
	}
	m, _ = clients[0].read("concWrite")
	// Ensure the contents are of the form "cl <i> 4"
	// The last write of any client ends with " 4"
	if !(m.Kind == 'C' && strings.HasSuffix(string(m.Contents), " 4")) {
		t.Fatalf("Expected to be able to read after 1000 writes. Got msg = %v", m)
	}

	err = exitTest()
	checkErr(t, err, "rpc_concurrent_writes: exittest")	
}

// nclients cas to the same file. At the end the file should be any one clients' last write.
// The only difference between this test and the ConcurrentWrite test above is that each
// client loops around until each CAS succeeds. The number of concurrent clients has been
// reduced to keep the testing time within limits.
func TestRPC_ConcurrentCas(t *testing.T) {
	var (
		nclients int
		niters int
		clients []*Client
		i int
		cl *Client
		errorCh chan error
		err error
		sem sync.WaitGroup // Used as a semaphore to coordinate goroutines to begin concurrently
		ver int
		m *Msg
	)

	err = initTest()
	checkErr(t, err, "rpc_concurrent_cas: inittest")

	err = startCluster()
	checkErr(t, err, "rpc_concurrent_cas: startcluster")
	time.Sleep(5 * time.Second)

	nclients = 5
	niters = 2

	clients = make([]*Client, nclients)
	for i = 0; i < nclients; i++ {
		cl = mkClient(t, 5001)
		if cl == nil {
			t.Fatalf("Unable to create client #%d", i)
		}
		defer cl.close()
		clients[i] = cl
	}

	sem.Add(1)

	m, _ = clients[0].write("concCas", "first", 0)
	ver = m.Version
	if m.Kind != 'O' || ver == 0 {
		t.Fatalf("Expected write to succeed and return version")
	}

	var wg sync.WaitGroup
	wg.Add(nclients)

	errorCh = make(chan error, nclients)
	
	for i = 0; i < nclients; i++ {
		go func(i int, ver int, cl *Client) {
			sem.Wait()
			defer wg.Done()
			for j := 0; j < niters; j++ {
				str := fmt.Sprintf("cl %d %d", i, j)
				for {
					m, err := cl.cas("concCas", ver, str, 0)
					if err != nil {
						errorCh <- err
						return
					} else if m.Kind == 'O' {
						break
					} else if m.Kind != 'V' {
						errorCh <- errors.New(fmt.Sprintf("Expected 'V' msg, got %c", m.Kind))
						return
					}
					ver = m.Version // retry with latest version
				}
				debug.Test("rpc_concurrent_cas:", ver)
			}
		}(i, ver, clients[i])
	}

	time.Sleep(100 * time.Millisecond) // give goroutines a chance
	sem.Done()                         // Start goroutines
	wg.Wait()                          // Wait for them to finish
	select {
	case e := <- errorCh:
		t.Fatalf("Error received while doing cas: %v", e)
	default: // no errors
	}
	m, _ = clients[0].read("concCas")

	if !(m.Kind == 'C' && strings.HasSuffix(string(m.Contents), " 1")) {
		t.Fatalf("Expected to be able to read after 1000 writes. Got msg.Kind = %d, msg.Contents=%s", m.Kind, m.Contents)
	}

	err = exitTest()
	checkErr(t, err, "rpc_concurrent_cas: exittest")	
}
////////////////////////////////////////////////////////////////////////////////////////////////////

// Node failure and recovery test case: 
// Client sends five write requests on the same file to the leader(Node-1)
// Node-2(Follower) is killed after the leader responses to all writes
// Now client sends the last write request on the same file to the leader
// Node-2 is restarted. Expectation is that Node-2 should recover and catch up with the leader for all requests
func TestSingleNodeFail(t *testing.T) {
	var (
		err error
		cl *Client
		txt string
		m *Msg
	)

	// Run cleaner. Install server executable.
	err = initTest()
	checkErr(t, err, "single_node_fail: inittest")

	// Start server processes
	err = startCluster()
	checkErr(t, err, "single_node_fail: startcluster")
	time.Sleep(5 * time.Second)

	// Connect client to node-1
	cl = mkClient(t, 5001)
	defer cl.close()

	// Write a file - cs733net
	txt = "foo1"
	m, err = cl.write("cs733net", txt, 0)
	expect(t, m, &Msg{Kind: 'O'}, "write success", err)	

	// Write the file again - cs733net
	txt = "foo2"
	m, err = cl.write("cs733net", txt, 0)
	expect(t, m, &Msg{Kind: 'O'}, "write success", err)	

	// Write the file again - cs733net
	txt = "foo3"
	m, err = cl.write("cs733net", txt, 0)
	expect(t, m, &Msg{Kind: 'O'}, "write success", err)	

	// Write the file again - cs733net
	txt = "foo4"
	m, err = cl.write("cs733net", txt, 0)
	expect(t, m, &Msg{Kind: 'O'}, "write success", err)	

	// Write the file again - cs733net
	txt = "foo5"
	m, err = cl.write("cs733net", txt, 0)
	expect(t, m, &Msg{Kind: 'O'}, "write success", err)	

	// Wait for node-2 to get the write
	time.Sleep(1 * time.Second)

	// Kill node-2
	err = killServer(2)
	if err != nil {
		t.Fatalf(err.Error())
	}
	debug.Test("single_node_fail: killserver 2")

	// Write the file again - cs733net
	txt = "foo6"
	m, err = cl.write("cs733net", txt, 0)
	expect(t, m, &Msg{Kind: 'O'}, "write success", err)	

	// Restart node-2
	err = begServer(2)
	if err != nil {
		t.Fatalf(err.Error())
	}
	debug.Test("single_node_fail: begserver 2")

	// Wait for node-2 to catch up
	time.Sleep(5 * time.Second)

	// Connect client to node-2
	cl = mkClient(t, 5002)

	// Read the file - cs733net. Node-2 should have the replication.
	m, err = cl.read("cs733net")
	expect(t, m, &Msg{Kind: 'C', Contents: []byte(txt)}, "read my write", err)

	// Run cleaner. 
	err = exitTest()
	checkErr(t, err, "single_node_fail: exittest")
}

// Leader failure and recovery test case:
// Client sends two write requests on the same file to the leader
// Once the leader sends the responses, it is killed
// Client sends the next two write requests on the same file to the new leader
// The new leader responds to these requests. Old leader is restarted now.
// Expectation is that the old leader catches up with the new one
func TestLeaderFail(t *testing.T) {
	var (
		err error
		cl *Client
		txt string
		m *Msg
		ldr int
		newId int
	)

	// Run cleaner. Install server executable.
	err = initTest()
	checkErr(t, err, "leader_fail: inittest")

	// Start server processes
	err = startCluster()
	checkErr(t, err, "leader_fail: startcluster")
	time.Sleep(5 * time.Second)

	// Connect client to node-1. Node-1 is the default server.
	// Client will be redirected to leader eventually.
	cl = mkClient(t, 5001)
	defer cl.close()	

	// Write a file - cs733net
	txt = "foo1"
	m, err = cl.write("cs733net", txt, 0)
	expect(t, m, &Msg{Kind: 'O'}, "write success 1", err)	

	// Write the same file - cs733net
	txt = "foo2"
	m, err = cl.write("cs733net", txt, 0)
	expect(t, m, &Msg{Kind: 'O'}, "write success 2", err)	

	// Get the leader id
	ldr = cl.serverPort % 5000

	// Kill the leader
	err = killServer(ldr)
	if err != nil {
		t.Fatalf(err.Error())
	}
	debug.Test("leader_fail: kill the leader")	

	// Wait for new leader formation
	time.Sleep(8 * time.Second)

	// Connect client to random server
	// It will connect to new leader eventually
	newId = ldr + 1
	if newId == 6 {
		newId = 1
	}
	cl = mkClient(t, 5000 + newId)	

	// Write the same file - cs733net
	txt = "foo3"
	m, err = cl.write("cs733net", txt, 0)
	expect(t, m, &Msg{Kind: 'O'}, "write success 3", err)		
	
	// Write the same file - cs733net
	txt = "foo4"
	m, err = cl.write("cs733net", txt, 0)
	expect(t, m, &Msg{Kind: 'O'}, "write success 4", err)		

	// Restart the old leader
	err = begServer(ldr)
	if err != nil {
		t.Fatalf(err.Error())
	}
	debug.Test("leader_fail: restart the leader")

	// Wait for old leader to catch up
	time.Sleep(5 * time.Second)

	// Connect client to old leader
	cl = mkClient(t, 5000 + ldr)

	// Read the file - cs733net. Old leader should have the contents foo4 with version 4
	m, err = cl.read("cs733net")
	if err != nil {
		t.Fatal("Unexpected error " + err.Error())
	}
	if !(string(m.Contents[:]) == txt && m.Version == 4) {
		t.Fatal("Expected msg with contents - foo4 with version 4. Received - " + string(m.Contents[:]) + strconv.Itoa(m.Version))
	}

	// Run cleaner. 
	err = exitTest()
	checkErr(t, err, "leader_fail: exittest")
}

// Utility constructs
func checkErr(t *testing.T, err error, msg ...interface{}) {
	if err != nil {
		t.Fatal(err, msg)
	}
}

func expect(t *testing.T, response *Msg, expected *Msg, errstr string, err error) {
	if err != nil {	
		t.Fatal("Unexpected error: " + errstr + ": " + err.Error())
	}
	ok := true
	if response.Kind != expected.Kind {
		ok = false
		errstr += fmt.Sprintf(" Got kind='%c', expected '%c'", response.Kind, expected.Kind)
	}
	if expected.Version > 0 && expected.Version != response.Version {
		ok = false
		errstr += " Version mismatch"
	}
	if response.Kind == 'C' {
		if expected.Contents != nil &&
			bytes.Compare(response.Contents, expected.Contents) != 0 {
			ok = false
		}
	}
	if !ok {
		t.Fatal("Expected " + errstr)
	}
}

type Msg struct {
	// Kind = the first character of the command. For errors, it
	// is the first letter after "ERR_", ('V' for ERR_VERSION, for
	// example), except for "ERR_CMD_ERR", for which the kind is 'M'
	Kind     byte
	Filename string
	Contents []byte
	Numbytes int
	Exptime  int // expiry time in seconds
	Version  int
	ClientConn *net.TCPConn
	ReDirPort int
}

var errNoConn = errors.New("Connection is closed")

type Client struct {
	conn   *net.TCPConn
	reader *bufio.Reader // a bufio Reader wrapper over conn
	serverPort int
}

func getConn(port int) (conn *net.TCPConn, err error){
	var (
		addr string
		raddr *net.TCPAddr
	)

	addr = "localhost:" + strconv.Itoa(port)
	raddr, err = net.ResolveTCPAddr("tcp", addr)
	if err == nil {
		conn, err = net.DialTCP("tcp", nil, raddr)
	}
	return conn, err
}

func getReader(conn *net.TCPConn) (*bufio.Reader) {
	return bufio.NewReader(conn)
}

func (cl *Client) read(filename string) (*Msg, error) {
	cmd := "read " + filename + "\r\n"
	return cl.sendRcv(cmd)
}

func (cl *Client) checkRT(msg *Msg) (res bool, err error) {
	if msg.Kind == 'R' || msg.Kind == 'T' {
		if msg.Kind == 'T' {
			time.Sleep(LDR_ELECT_TIME * time.Millisecond)
			err = nil
		} else {
			cl.close()
			cl.conn, err = getConn(msg.ReDirPort)
			if err == nil {
				cl.reader = getReader(cl.conn)
				cl.serverPort = msg.ReDirPort
			}
		}
		return true, err
	}
	return false, nil
}

func (cl *Client) write(filename string, contents string, exptime int) (msg *Msg, err error) {
	var (
		cmd string
		res bool
	)

	if exptime == 0 {
		cmd = fmt.Sprintf("write %s %d\r\n", filename, len(contents))
	} else {
		cmd = fmt.Sprintf("write %s %d %d\r\n", filename, len(contents), exptime)
	}
	cmd += contents + "\r\n"
	msg, err = cl.sendRcv(cmd)
	if err == nil {
		res, err = cl.checkRT(msg)
		if res && err == nil {
			return cl.write(filename, contents, exptime)
		}
	}
	return msg, err
}

func (cl *Client) cas(filename string, version int, contents string, exptime int) (msg *Msg, err error) {
	var (
		cmd string
		res bool
	)

	if exptime == 0 {
		cmd = fmt.Sprintf("cas %s %d %d\r\n", filename, version, len(contents))
	} else {
		cmd = fmt.Sprintf("cas %s %d %d %d\r\n", filename, version, len(contents), exptime)
	}
	cmd += contents + "\r\n"
	msg, err = cl.sendRcv(cmd)
	if err == nil {
		res, err = cl.checkRT(msg)
		if res && err == nil {
			return cl.cas(filename, version, contents, exptime)
		}
	}
	return msg, err
}

func (cl *Client) delete(filename string) (msg *Msg, err error) {
	var (
		cmd string
		res bool
	)

	cmd = "delete " + filename + "\r\n"
	msg, err = cl.sendRcv(cmd)
	if err == nil {
		res, err = cl.checkRT(msg)
		if res && err == nil {
			return cl.delete(filename)
		}
	}
	return msg, err
}

func mkClient(t *testing.T, port int) (client *Client) {
	var (
		addr string
		raddr *net.TCPAddr
		err error
		conn *net.TCPConn
	)

	addr = "localhost:" + strconv.Itoa(port)
	raddr, err = net.ResolveTCPAddr("tcp", addr)
	if err == nil {
		conn, err = net.DialTCP("tcp", nil, raddr)
		if err == nil {
			client = &Client{conn: conn, reader: bufio.NewReader(conn), serverPort: port}
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func (cl *Client) send(str string) error {
	if cl.conn == nil {
		return errNoConn
	}
	_, err := cl.conn.Write([]byte(str))
	if err != nil {
		err = fmt.Errorf("Write error in SendRaw: %v", err)
		cl.conn.Close()
		cl.conn = nil
	}
	return err
}

func (cl *Client) sendRcv(str string) (msg *Msg, err error) {
	if cl.conn == nil {
		return nil, errNoConn
	}
	err = cl.send(str)
	if err == nil {
		msg, err = cl.rcv()
	}
	return msg, err
}

func (cl *Client) close() {
	if cl != nil && cl.conn != nil {
		cl.conn.Close()
		cl.conn = nil
	}
}

func (cl *Client) rcv() (msg *Msg, err error) {
	// we will assume no errors in server side formatting
	line, err := cl.reader.ReadString('\n')
	if err == nil {
		msg, err = parseFirst(line)
		if err != nil {
			return nil, err
		}
		if msg.Kind == 'C' {
			contents := make([]byte, msg.Numbytes)
			var c byte
			for i := 0; i < msg.Numbytes; i++ {
				if c, err = cl.reader.ReadByte(); err != nil {
					break
				}
				contents[i] = c
			}
			if err == nil {
				msg.Contents = contents
				cl.reader.ReadByte() // \r
				cl.reader.ReadByte() // \n
			}
		}
	}
	if err != nil {
		cl.close()
	}
	return msg, err
}

func parseFirst(line string) (msg *Msg, err error) {
	fields := strings.Fields(line)
	msg = &Msg{}

	// Utility function fieldNum to int
	toInt := func(fieldNum int) int {
		var i int
		if err == nil {
			if fieldNum >=  len(fields) {
				err = errors.New(fmt.Sprintf("Not enough fields. Expected field #%d in %s\n", fieldNum, line))
				return 0
			}
			i, err = strconv.Atoi(fields[fieldNum])
		}
		return i
	}

	if len(fields) == 0 {
		return nil, errors.New("Empty line. The previous command is likely at fault")
	}
	switch fields[0] {
	case "OK": // OK [version]
		msg.Kind = 'O'
		if len(fields) > 1 {
			msg.Version = toInt(1)
		}
	case "CONTENTS": // CONTENTS <version> <numbytes> <exptime> \r\n
		msg.Kind = 'C'
		msg.Version = toInt(1)
		msg.Numbytes = toInt(2)
		msg.Exptime = toInt(3)
	case "ERR_VERSION":
		msg.Kind = 'V'
		msg.Version = toInt(1)
	case "ERR_FILE_NOT_FOUND":
		msg.Kind = 'F'
	case "ERR_CMD_ERR":
		msg.Kind = 'M'
	case "ERR_INTERNAL":
		msg.Kind = 'I'	
	case "ERR_REDIRECT":
		msg.Kind = 'R'
		msg.ReDirPort = toInt(2)
	case "ERR_TRY_LATER":
		msg.Kind = 'T'			
	default:
		err = errors.New("Unknown response " + fields[0])
	}
	if err != nil {
		return nil, err
	} else {
		return msg, nil
	}
}