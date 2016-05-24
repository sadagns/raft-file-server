package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"encoding/json"
	"encoding/gob"	
	"bytes"
	"sync"
	"github.com/sadagopanns/cs733/assignment4/fs"
	"github.com/sadagopanns/cs733/assignment4/raft"
	"github.com/sadagopanns/cs733/assignment4/utils"
	"github.com/sadagopanns/cs733/assignment4/debug"
	// "time"
)

// Mapping unique client ids to its tcp connection. Used by file server in sending the response.
type connMap struct {
	sync.Mutex
	m map[int]*net.TCPConn
}

const (
	MAX_CLIENTS = 1000000000
)

var (
	selfId int
	rn raft.Node
	host string
	port int
	cmap connMap
	crlf = []byte{'\r', '\n'}
)

// Create the map
func (cm *connMap) new() {
	cm.Lock()
	defer cm.Unlock()
	cm.m = make(map[int]*net.TCPConn)
}

// Add a new entry
func (cm *connMap) add(key int, val *net.TCPConn) {
	cm.Lock()
	defer cm.Unlock()
	cm.m[key] = val
}

// Get the value for a key
func (cm *connMap) get(key int) (val *net.TCPConn, ok bool) {
	cm.Lock()
	defer cm.Unlock()
	val, ok = cm.m[key]
	return val, ok
}

// Remove an entry
func (cm *connMap) rem(key int) {
	cm.Lock()
	defer cm.Unlock()
	delete(cm.m, key)
}

// Check for non-nil fatal errors
func check(obj interface{}) {
	if obj != nil {
		fmt.Println(obj)
		os.Exit(1)
	}
}

// Providing response to all client requests
func reply(conn *net.TCPConn, msg *fs.Msg) bool {
	var (
		err error
		resp string
	)

	write := func(data []byte) {
		if err != nil {
			return
		}
		_, err = conn.Write(data)
	}

	// Processing based on msg kind
	switch msg.Kind {

	// Read
	case 'C': 
		resp = fmt.Sprintf("CONTENTS %d %d %d", msg.Version, msg.Numbytes, msg.Exptime)

	// Write
	case 'O':
		resp = "OK "
		if msg.Version > 0 {
			resp += strconv.Itoa(msg.Version)
		}

	// Incorrect file 
	case 'F':
		resp = "ERR_FILE_NOT_FOUND"

	// Incorrect version. Required for CAS.
	case 'V':
		resp = "ERR_VERSION " + strconv.Itoa(msg.Version)

	// Erroneous command received
	case 'M':
		resp = "ERR_CMD_ERR"

	// Unexpected behaviour at server
	case 'I':
		resp = "ERR_INTERNAL"

	// Server in follower state
	case 'R':
		resp = "ERR_REDIRECT " + host + " " + strconv.Itoa(msg.ReDirPort)

	// Leader not yet found	
	case 'T':
		resp = "ERR_TRY_LATER"

	// Erroneous kind
	default:
		fmt.Printf("Unknown response kind '%c'", msg.Kind)
		return false
	}

	// End characters
	resp += "\r\n"

	// Send response
	write([]byte(resp))
	if msg.Kind == 'C' {
		write(msg.Contents)
		write(crlf)
	}

	return err == nil
}

// Converting msg struct to bytes
func encode(msg *fs.Msg) (data []byte, err error) {
	var (
		buf *bytes.Buffer
		enc *gob.Encoder
	)
	
	buf = &bytes.Buffer{}
	enc = gob.NewEncoder(buf)
	err = enc.Encode(msg)
	return buf.Bytes(), err
}

// Converting bytes to msg struct
func decode(data []byte) (msg *fs.Msg, err error) {
	var (
		buf *bytes.Buffer
		dec *gob.Decoder
	)

	msg = &fs.Msg{}
	buf = bytes.NewBuffer(data)
	dec = gob.NewDecoder(buf)
	err = dec.Decode(msg)
	return msg, err
}

// Close tcpconn. Remove client id from map.
func closeClient(conn *net.TCPConn, clientId int) {
	conn.Close()
	cmap.rem(clientId)
}

// Accept all client's requests
func serve(conn *net.TCPConn, clientId int) {
	var (
		reader *bufio.Reader
		msg *fs.Msg
		msgerr error
		fatalerr error			
		response *fs.Msg
		err error
		data []byte
	)

	// Initialization
	reader = bufio.NewReader(conn)

	for {
		// Read a complete request and form a msg struct
		msg, msgerr, fatalerr = fs.GetMsg(reader)

		// Handle errors
		if fatalerr != nil || msgerr != nil {
			reply(conn, &fs.Msg{Kind: 'M'})
			closeClient(conn, clientId)
			break
		}
		if msgerr != nil {
			if (!reply(conn, &fs.Msg{Kind: 'M'})) {
				closeClient(conn, clientId)
				break
			}
		}
		debug.Server(selfId, "server: received msg:", string(msg.Kind), msg.Filename, string(msg.Contents[:]))

		// Check if read request received
		if msg.Kind == 'r' {

			// Process and reply read request
			response = fs.ProcessMsg(msg)
			if !reply(conn, response) {
				closeClient(conn, clientId)
				break
			}

		// Write/CAS/Delete requests: Append to RAFT.
		} else {
			msg.ClientId = clientId
			data, err = encode(msg)
			if err != nil {
				debug.Server(selfId, "serve: encode: interror:", err)
				reply(conn, &fs.Msg{Kind: 'I'})
				closeClient(conn, clientId)
				break
			}
			rn.Append(data)
		}
	}
}

// Listen and accept clients
func startServer() {
	var (
		tcpaddr *net.TCPAddr
		err error
		tcp_acceptor *net.TCPListener
		tcp_conn *net.TCPConn
		addr string
		clientId int
	)

	// Create client connection map
	cmap.new()

	// Initialization
	clientId = 0
	host = "localhost"
	port = 5000 + selfId
	addr = host + ":" + strconv.Itoa(port)

	// Listen for clients
	tcpaddr, err = net.ResolveTCPAddr("tcp", addr)
	check(err)
	tcp_acceptor, err = net.ListenTCP("tcp", tcpaddr)
	check(err)

	// Keep accepting clients and update map
	for {
		clientId = (clientId + 1) % MAX_CLIENTS
		tcp_conn, err = tcp_acceptor.AcceptTCP()
		check(err)
		cmap.add(clientId, tcp_conn)

		// Serve client in a separate goroutine
		go serve(tcp_conn, clientId)
	}	
}

// Listen for committed client requests, process and send reply
func startFs() {
	var (
		ch <- chan raft.CommitInfo
		ok bool
		ci raft.CommitInfo
		lastConInd int64
		msg *fs.Msg
		conn *net.TCPConn
		i int64
		ldrPort int
		entry raft.LogEntry
		err error
		response *fs.Msg
	)

	ch = rn.CommitChannel()
	lastConInd = -1
	for {
		// Receive from commit channel
		ci = <- ch
		debug.Server(selfId, "startfs: <- ch:", ci.LeaderId, ci.Index, ci.Err)

		// Check for errors
		if ci.Err != nil {

			// Decode
			msg, err = decode(ci.Entry.Data)
			utils.Assert(err == nil, err, "startfs: decode error:")

			// Get client conn
			conn, ok = cmap.get(msg.ClientId)
			utils.Assert(ok, "startfs: ci.err: no clientid found for this msg:", string(msg.Kind), msg.Filename, string(msg.Contents[:]), ci.Err) 

			// Reply with error msgs
			switch ci.Err.Error() {

			// Redirect err
			case "ERR_CONTACT_LEADER":
				ldrPort = 5000 + ci.LeaderId
				reply(conn, &fs.Msg{Kind: 'R', ReDirPort: ldrPort})

			// Try later err: Leader still not found
			case "ERR_TRY_LATER":
				reply(conn, &fs.Msg{Kind: 'T'})

			default:
				utils.Assert(false, ci.Err)
			}

		// Commited log entry arrived
		} else {

			// Handle all committed log entries
			for i = lastConInd + 1; i <= ci.Index; i++ {

				// Get entry 
				entry, err = rn.Get(i)
				utils.Assert(err == nil, err)

				// Decode
				msg, err = decode(entry.Data)
				utils.Assert(err == nil, err)
				debug.Server(selfId, "startfs: received msg:", string(msg.Kind), msg.Filename, string(msg.Contents[:]))

				// Process msg
				response = fs.ProcessMsg(msg)

				// Check for this client id
				conn, ok = cmap.get(msg.ClientId)
				
				// Reply only if you have a client conn
				if ok {
					utils.Assert(ci.LeaderId == selfId, "startfs: clientid found but it is not a leader")
					reply(conn, response)
				}					
			}
			// Update last consumer index
			lastConInd = utils.Max(lastConInd, ci.Index)
		}
	}
}

// Get config for RAFT from json file
func getConfig() (cfg *raft.Config, err error) {
	var (
		cf *os.File
		filename string
		dec *json.Decoder
	)

	filename = "config/config_" + strconv.Itoa(selfId) + ".json"
	if cf, err = os.Open(filename); err != nil {
		return nil, err
	}
	defer cf.Close()
	dec = json.NewDecoder(cf)
	if err = dec.Decode(&cfg); err != nil {
		return nil, err
	}
	cfg.Id = selfId
	cfg.LogDir = strconv.Itoa(cfg.Id)
	return cfg, nil	
}

// Create a raft node. This begins raft consensus.
func startRaft() {
	var (
		cfg *raft.Config
		err error
	)

	cfg, err = getConfig()
	utils.Assert(err == nil, "startRn: Error in config.json")
	rn = raft.New(cfg)
}

// Starting point for the server
func serverMain(arg int) {
	selfId = arg
	startRaft()
	go startServer()
	startFs()
}

func main() {
	var (
		args []string
		arg int
		err error
	)

	// Get the server id and start server operations
	args = os.Args[1:]
	utils.Assert(len(args) == 1, "startRn: One arg expected")	
	arg, err = strconv.Atoi(args[0])
	utils.Assert(err == nil, "startRn: Invaid arg")
	serverMain(arg)
}