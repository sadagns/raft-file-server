package raft

import(
	"time"
	"strconv"
	"os"
	"encoding/gob"
	"sync"
	"errors"
	"github.com/cs733-iitb/log"
	"github.com/cs733-iitb/cluster"
	"github.com/cs733-iitb/cluster/mock"	
	"github.com/sadagopanns/cs733/assignment4/utils"
	// "github.com/sadagopanns/cs733/assignment4/debug"
)

const (
	ChBufSize = 10000000
)

type CommitInfo CommitAction
type StateInfo StateStoreAction

// Address structure of RAFT node
type NetConfig struct {
	Id int
	Host string
	Port int
}

// Configuration of cluster
type Config struct {
	Cl []NetConfig
	Id int
	LogDir string
	ElectionTimeout int64
	HeartbeatTimeout int64
}

// RAFT node structure
type RaftNode struct {
	// Raft State Machine and its lock
	sync.Mutex
	rsm RaftStateMachine

	// Path string for logs
	fileDir string

	// Timer
	timer *time.Timer

	// Server for communication with the rest of the cluster
	server cluster.Server

	// Channels for handling various events
	appendCh chan AppendEvent
	CommitCh chan CommitInfo
	quitCh chan int

	// Disk store files
	logFile *log.Log
	stateFile stateIo
}

// RAFT node API
type Node interface {
	// Create a mock cluster
	Mock(*mock.MockServer)

	// Append data into append channel
	Append([]byte)

	// Get commitchannel
	CommitChannel() (<- chan CommitInfo)

	// Get committed index of rsm
	CommittedIndex() (int64)

	// Get log entry
	Get(int64) (LogEntry, error)

	// Get id of rsm
	Id() (int)

	// Get leaderid of rsm
	LeaderId() (int)

	// Shut everything
	Shutdown()
}

// Get cluster config
func getClusterConfig(config *Config) cluster.Config {
	var (
		i int
		id int
		address string
		inboxSize int
		outboxSize int
		configFile cluster.Config
		peers []cluster.PeerConfig
	)

	for i = 0; i < len(config.Cl); i++ {
		id = config.Cl[i].Id
		address = config.Cl[i].Host + ":" + strconv.Itoa(config.Cl[i].Port)
		peers = append(peers, cluster.PeerConfig{id, address})
	}
	inboxSize = 10000
	outboxSize = 10000
	configFile = cluster.Config{peers, inboxSize, outboxSize}
	return configFile
} 

// Get initial peer ids info for Raft State Machine
func getPeerIds(config *Config) []int {
	var (
		i int
		id int
		peerIds []int
	)

	for i = 0; i < len(config.Cl); i++ {
		id = config.Cl[i].Id
		if id != config.Id {
			peerIds = append(peerIds, id)
		}
	}
	return peerIds
}

// Get initial votes received info for Raft State Machine
func getVotesRcvd(peerIds []int) map[int]int {
	var (
		i int
		id int
		votesRcvd map[int]int
	)

	votesRcvd = make(map[int]int)
	for i = 0; i < len(peerIds); i++ {
		id = peerIds[i]
		votesRcvd[id] = 0
	}
	return votesRcvd
}

// Get initial next index info for Raft State Machine
func getNextIndex(peerIds []int) map[int]int64 {
	var (
		i int
		id int
		nextIndex map[int]int64
	)

	nextIndex = make(map[int]int64)
	for i = 0; i < len(peerIds); i++ {
		id = peerIds[i]
		nextIndex[id] = -1
	}
	return nextIndex
}

// Get initial match index info for Raft State Machine
func getMatchIndex(peerIds []int) map[int]int64 {
	var (
		i int
		id int
		matchIndex map[int]int64
	)

	matchIndex = make(map[int]int64)
	for i = 0; i < len(peerIds); i++ {
		id = peerIds[i]
		matchIndex[id] = -1
	}
	return matchIndex
}

// Register structures for gob. Used in cluser and log packages.
func registerStructs() {
	gob.Register(LogEntry{})
	gob.Register(AppendEntriesReqEvent{})
	gob.Register(AppendEntriesRespEvent{})
	gob.Register(VoteReqEvent{})
	gob.Register(VoteRespEvent{})
	gob.Register(SendAction{})
	gob.Register(StateInfo{})
}

// Creates a new RAFT node
func New(config *Config) Node {
	var(
		rn RaftNode
		err error
		rsmLog []LogEntry
		rsmState StateInfo
		peerIds []int
		votesRcvd map[int]int
		nextIndex map[int]int64
		matchIndex map[int]int64
		configFile cluster.Config
		rsmLastLogIndex int64
		rsmLastLogTerm int
	)

	// Make required channels
	rn.appendCh = make(chan AppendEvent, ChBufSize)
	rn.CommitCh = make(chan CommitInfo, ChBufSize)
	rn.quitCh = make(chan int)

	// Initialization
	configFile = getClusterConfig(config)
	peerIds = getPeerIds(config)
	votesRcvd = getVotesRcvd(peerIds)
	nextIndex = getNextIndex(peerIds)
	matchIndex = getMatchIndex(peerIds)

	// Set the path for log/state
	rn.fileDir = config.LogDir

	// Register struct for gob
	registerStructs()

	// Start server to communicate in the cluster
	rn.server, err = cluster.New(config.Id, configFile)
	utils.Assert(err == nil, "New: clusterNew")

	// Open log and get log entries
	rn.logFile, err = log.Open(rn.fileDir + "/log")
	utils.Assert(err == nil, "New: logOpen")
	rsmLog, err = rn.getRsmLog();
	utils.Assert(err == nil, "New: getRsmLog")

	// Open statelog and get state variables
	rn.stateFile, err = newStateIo(rn.fileDir + "/state")
	utils.Assert(err == nil, "New: newStateIo")
	rsmState, err = rn.getRsmState();
	utils.Assert(err == nil, "New: getRsmState")

	// Initializing rsm.lastLogIndex and rsm.lastLogTerm from persistence values
	rsmLastLogIndex = rsmState.LastRepIndex
	if rsmLastLogIndex == -1 {
		rsmLastLogTerm = 0
	} else {
		rsmLastLogTerm = rsmLog[rsmLastLogIndex].Term
	}

	// Start Raft State Machine
	rn.rsm.init(/* currTerm */ rsmState.CurrTerm, /* votedFor */ rsmState.VotedFor, /* log */ rsmLog, /* selfId */ config.Id, /* peerIds */ peerIds, /* electionAlarmPeriod */ config.ElectionTimeout, /* heartbeatAlarmPeriod */ config.HeartbeatTimeout, /* lastRepIndex */ rsmState.LastRepIndex, /* currState */ "Follower", /* commitIndex */ -1, /* leaderId */ -1, /* lastLogIndex */ rsmLastLogIndex, /* lastLogTerm */ rsmLastLogTerm, /* votesRcvd */ votesRcvd, /* nextIndex */ nextIndex, /* matchIndex */ matchIndex)

	// Start election timer
	rn.timer = time.NewTimer(time.Duration(utils.RandRange(config.ElectionTimeout, 2 * config.ElectionTimeout)) * time.Millisecond)

	// Run RAFT in background
	go rn.run()

	return &rn
}

// Get initial log info for Raft State Machine from persistent disk log storage
func (rn *RaftNode) getRsmLog() ([]LogEntry, error) {
	var (
		rsmLog []LogEntry
		entry LogEntry
		itf interface{}
		err error
		lastInd int64
		i int64
	)

	lastInd = rn.logFile.GetLastIndex()
	for i = 0; i <= lastInd; i++ {
		itf, err = rn.logFile.Get(i)
		if err != nil {
			return []LogEntry{}, err
		}
		entry = itf.(LogEntry)
		rsmLog = append(rsmLog, entry)
	}
	return rsmLog, nil
}

// Get initial state info for Raft State Machine from persistent disk log storage
func (rn *RaftNode) getRsmState() (StateInfo, error) {
	var (
		rsmStateStore StateStoreAction
		err error
	)

	rsmStateStore, err = rn.stateFile.readState()
	if err != nil && err.Error() == "ERR_NOT_SET" {
		err = nil
		rsmStateStore.CurrTerm = 0
		rsmStateStore.VotedFor = -1
		rsmStateStore.LastRepIndex = -1
	}
	return StateInfo(rsmStateStore), err
}

// Used for Mock cluster testing
func (rn *RaftNode) Mock(mServer *mock.MockServer) {
	rn.server.Close()
	rn.server = mServer
}

// Append data into append channel
func (rn *RaftNode) Append(data []byte) {
	rn.appendCh <- AppendEvent{data}
}

// Return commit channel of RAFT node
func (rn *RaftNode) CommitChannel() (<- chan CommitInfo) {
	return rn.CommitCh
}

// Return committed index
func (rn *RaftNode) CommittedIndex() (int64) {
	rn.Lock()
	defer rn.Unlock()
	return rn.rsm.CommitIndex
}

// Return log entry from log of Raft State Machine
func (rn *RaftNode) Get(index int64) (entry LogEntry, err error) {
	rn.Lock()
	defer rn.Unlock()
	if int(index) < len(rn.rsm.log) {
		entry = rn.rsm.log[index]
		err = nil
	} else {
		err = errors.New("ERR_IND_OUT_OF_RANGE")
	}
	return entry, err
}

// Return self id
func (rn *RaftNode) Id() (int) {
	rn.Lock()
	defer rn.Unlock()
	return rn.rsm.selfId
}

// Return leader id 
func (rn *RaftNode) LeaderId() (int) {
	rn.Lock()
	defer rn.Unlock()
	return rn.rsm.leaderId
}

// Shutdown RAFT node
func (rn *RaftNode) Shutdown() {
	close(rn.quitCh)		
}

// Start RAFT node processing
func (rn *RaftNode) run() {
	var (
		res bool
		event Event
		envelope *cluster.Envelope
		actions []Action
	)

	for {
		// Process the happening event
		select {

		// Append data event
		case event = <- rn.appendCh:

		// Timeout event
		case <- rn.timer.C:
			event = TimeoutEvent{}

		// Message from peer
		case envelope, _ = <- rn.server.Inbox():
			event = envelope.Msg

		// Shutdown
		case _, res = <- rn.quitCh:
			if res == false {
				break
			}
		}

		// Get the lock and process the event
		rn.Lock()
		actions = rn.rsm.ProcessEvent(event)
		rn.Unlock()

		// Process event's actions
		rn.handleActions(actions)
	}

	// Shutting down
	rn.timer.Stop()
	rn.server.Close()
	close(rn.appendCh)
	close(rn.CommitCh)
	rn.logFile.Close()
	rn.stateFile.close()
	cleanDir(rn.fileDir)		
}

// Handle actions from Raft State Machine 
func (rn *RaftNode) handleActions(actions []Action) {
	var (
		i int
		res bool
		err error
		action Action
		send SendAction
		commit CommitAction
		alarm AlarmAction
		logStore LogStoreAction
		stateStore StateStoreAction
	)

	// Process actions
	for i = 0; i < len(actions); i++ {
		action = actions[i]

		// Process based on action type
		switch action.(type) {

		// Send peer
		case SendAction:
			send = action.(SendAction)
			rn.server.Outbox() <- &cluster.Envelope{Pid: send.PeerId, MsgId: -1, Msg: send.Ev}

		// Push Commit info into Commit channel
		case CommitAction:
			commit = action.(CommitAction)
			rn.CommitCh <- CommitInfo(commit)

		// Reset/Renew timer
		case AlarmAction:
			alarm = action.(AlarmAction)
			res = rn.timer.Reset(time.Duration(alarm.AlarmPeriod) * time.Millisecond)
			if !res {
				rn.timer = time.NewTimer(time.Duration(alarm.AlarmPeriod) * time.Millisecond)
			}

		// Log store
		case LogStoreAction:
			logStore = action.(LogStoreAction)
			err = rn.logFile.TruncateToEnd(logStore.Index)
			utils.Assert(err == nil, "handleActions: LogStoreAction")
			err = rn.logFile.Append(logStore.Entry)
			utils.Assert(err == nil, "handleActions: LogStoreAction")		

		// State store
		case StateStoreAction:
			stateStore = action.(StateStoreAction)
			err = rn.stateFile.writeState(stateStore)
			utils.Assert(err == nil, "handleActions: StateStoreAction")	
		}
	}
}

// Remove all log/state directories
func cleanDir(dir string) {
	os.RemoveAll(dir)
}