package raft

import (
	"errors"
	"github.com/sadagopanns/cs733/assignment4/utils"
	"github.com/sadagopanns/cs733/assignment4/debug"
)

const (
	lb = 2
	ub = 3
)

type Event interface{}

type Action interface{}

// Log entry structure
type LogEntry struct {
	Term int
	Data []byte
}

// Append structure
type AppendEvent struct {
	Data []byte
}

// Timeout structure
type TimeoutEvent struct {
	
}

// Append entries request structure
type AppendEntriesReqEvent struct {
	SenderId int
	SenderTerm int
	PrevLogIndex int64
	PrevLogTerm int
	Entries []LogEntry
	SenderCommitIndex int64
}

// Append entries response structure
type AppendEntriesRespEvent struct {
	SenderId int
	SenderTerm int
	SenderLastRepIndex int64 /* the last index upto which the sender's log matches the leader's log (in the current term) - Applicable if response - true */
	Response bool
}

// Vote request structure
type VoteReqEvent struct {
	SenderId int
	SenderTerm int
	SenderLastLogIndex int64
	SenderLastLogTerm int
}

// Vote response structure
type VoteRespEvent struct {
	SenderId int
	SenderTerm int
	Response bool	
}

// Send structure
type SendAction struct {
	PeerId int
	Ev Event
}

// Commit structure
type CommitAction struct {
	LeaderId int /* Applicable if the server is not a leader */
	Index int64
	Entry LogEntry
	Err error
}

// Alarm structure
type AlarmAction struct {
	AlarmPeriod int64
}

// Log store structure
type LogStoreAction struct {
	Index int64
	Entry LogEntry
}

// State store structure
type StateStoreAction struct {
	CurrTerm int
	VotedFor int
	LastRepIndex int64
}

// Raft State Machine structure
type RaftStateMachine struct {
	// Persistent: Applicable to servers on any state
	currTerm int
	votedFor int
	log []LogEntry
	selfId int
	peerIds []int
	electionAlarmPeriod int64
	heartbeatAlarmPeriod int64

	// Persistent: Applicable to servers on Follower state
	lastRepIndex int64 /* In a term, lastRepIndex denotes the last index upto which the log matches the leader's log (in the current term)*/

	// Non-Persistent: Applicable to servers on any state
	currState string
	CommitIndex int64
	leaderId int
	lastLogIndex int64
	lastLogTerm int
	clusterSize int

	// Non-Persistent: Applicable to servers on Candidate state
	votesRcvd map[int]int /* For a server, 0 : No voteResponse received, 1 : voteGranted, -1 : voteDenied */

	// Non-Persistent: Applicable to a server on Leader state 
	// Maps from senderId to corresponding variable
	nextIndex map[int]int64
	matchIndex map[int]int64
}

// Start the Raft State Machine with the initial values
func (rsm *RaftStateMachine) init(currTerm int, votedFor int, log []LogEntry, selfId int, peerIds []int, electionAlarmPeriod int64, heartbeatAlarmPeriod int64, lastRepIndex int64, currState string, commitIndex int64, leaderId int, lastLogIndex int64, lastLogTerm int, votesRcvd map[int]int, nextIndex map[int]int64, matchIndex map[int]int64) {
	rsm.currTerm = currTerm
	rsm.votedFor = votedFor
	rsm.log = log
	rsm.selfId = selfId
	rsm.peerIds = peerIds	
	rsm.electionAlarmPeriod = electionAlarmPeriod
	rsm.heartbeatAlarmPeriod = heartbeatAlarmPeriod
	rsm.lastRepIndex = lastRepIndex
	rsm.currState = currState
	rsm.CommitIndex = commitIndex
	rsm.leaderId = leaderId
	rsm.lastLogIndex = lastLogIndex
	rsm.lastLogTerm = lastLogTerm
	rsm.clusterSize = len(peerIds) + 1
	rsm.votesRcvd = votesRcvd
	rsm.nextIndex = nextIndex
	rsm.matchIndex = matchIndex
}

// Process event from RAFT node
func (rsm *RaftStateMachine) ProcessEvent(event Event) []Action {
	var (
		actions []Action
	)

	// Processing based on event type
	switch event.(type) {

	// Append 
	case AppendEvent:
		actions = rsm.handleAppend(event)

	// Timeout
	case TimeoutEvent:
		actions = rsm.handleTimeout(event)

	// Append Entries Request
	case AppendEntriesReqEvent:
		actions = rsm.handleAppendEntriesReq(event)

	// Append Entries Response
	case AppendEntriesRespEvent:
		actions = rsm.handleAppendEntriesResp(event)

	// Vote Request	
	case VoteReqEvent:
		actions = rsm.handleVoteReq(event)

	// Vote Response
	case VoteRespEvent:
		actions = rsm.handleVoteResp(event)
	}
	return actions
}

// Send append entries request
func (rsm *RaftStateMachine) sendAppendEntriesRPC(serverIds []int) []Action {
	var (
		actions []Action
		appendEntriesReq AppendEntriesReqEvent
		send SendAction
		i int
		prevLogIndex int64
		prevLogTerm int
		entries []LogEntry
	)

	for i = 0; i < len(serverIds); i++ {
		// Set prevlogindex and prevlogterm
		prevLogIndex = rsm.nextIndex[serverIds[i]] - 1
		if prevLogIndex < 0 {
			prevLogTerm = 0
		} else if prevLogIndex >= int64(len(rsm.log)) {
			// Added for preventing errors. No idea how such condition occurs during go test
			continue
		} else {
			prevLogTerm = rsm.log[prevLogIndex].Term
		}

		// Error handling
		if rsm.nextIndex[serverIds[i]] > int64(len(rsm.log)) {
			// Added for preventing errors. No idea how such condition occurs during go test
			continue
		}

		// debug.Print(rsm.selfId, rsm.leaderId, rsm.currTerm, "send_append_entries_rpc:", serverIds[i], prevLogIndex, prevLogTerm, len(rsm.log) - 1, rsm.nextIndex[serverIds[i]])
		// debug.Print(rsm.selfId, rsm.leaderId, rsm.currTerm, "send_append_entries_rpc:", rsm.CommitIndex)

		// Make entries to be sent
		entries = rsm.log[rsm.nextIndex[serverIds[i]]:]
		appendEntriesReq = AppendEntriesReqEvent{rsm.selfId, rsm.currTerm, prevLogIndex, prevLogTerm, entries, rsm.CommitIndex}

		// Send entries
		send = SendAction{serverIds[i], appendEntriesReq}
		actions = append(actions, send)
	}
	return actions
}

// Send vote request
func (rsm *RaftStateMachine) sendVoteRequestRPC(serverIds []int) []Action {
	var (
		actions []Action
		voteReq VoteReqEvent
		send SendAction
		i int
	)

	for i = 0; i < len(serverIds); i++ {
		voteReq = VoteReqEvent{rsm.selfId, rsm.currTerm, rsm.lastLogIndex, rsm.lastLogTerm}
		send = SendAction{serverIds[i], voteReq}
		actions = append(actions, send)
	}
	return actions
}

// Append event
func (rsm *RaftStateMachine) handleAppend(event Event) []Action {
	var (
		appendArg AppendEvent
		actions []Action
		data []byte
		entry LogEntry
		logStore LogStoreAction
		commit CommitAction
	)

	// Initialization
	appendArg, _ = event.(AppendEvent)
	data = appendArg.Data

	// Processing based on state
	switch rsm.currState {

	// Follower - Redirect / Retry
	case "Follower":

		debug.Rsm("APP: F: 1", "id:", rsm.selfId, "term:", rsm.currTerm)

		// Response - redirect / retry based on leader selection status
		if rsm.leaderId == -1 {
			commit = CommitAction{-1, -1, LogEntry{-1, data}, errors.New("ERR_TRY_LATER")}			
		} else {
			commit = CommitAction{rsm.leaderId, -1, LogEntry{-1, data}, errors.New("ERR_CONTACT_LEADER")}
		}
		actions = append(actions, commit)

	// Candidate - Retry
	case "Candidate":

		// Response - retry
		debug.Rsm("APP: C: 1", "id:", rsm.selfId, "term:", rsm.currTerm)
		commit = CommitAction{-1, -1, LogEntry{-1, data}, errors.New("ERR_TRY_LATER")}
		actions = append(actions, commit)

	// Main state for this event
	case "Leader":

		debug.Rsm("APP: L: 1", "id:", rsm.selfId, "term:", rsm.currTerm)

		// Updated log variables
		rsm.lastLogIndex++
		rsm.lastLogTerm = rsm.currTerm
		entry = LogEntry{rsm.currTerm, data}

		// Append log entry
		rsm.log = append(rsm.log, entry)

		// Log store
		logStore = LogStoreAction{rsm.lastLogIndex, entry}
		actions = append(actions, logStore)	

		// Send append RPC
		actions = append(actions, rsm.sendAppendEntriesRPC(rsm.peerIds)...)
	}
	return actions
}	

// Timeout event
func (rsm *RaftStateMachine) handleTimeout(event Event) []Action {
	var (
		actions []Action
		stateStore StateStoreAction
		alarm AlarmAction
		i int
	)

	// Processing based on state
	switch rsm.currState {

	// Same behaviour for both follower and candidate
	case "Follower", "Candidate":

		// debug.Print(rsm.selfId, rsm.leaderId, rsm.currTerm, "tout: follower/candidate")

		// Reset election alarm
		alarm = AlarmAction{utils.RandRange(lb * rsm.electionAlarmPeriod, ub * rsm.electionAlarmPeriod)}
		actions = append(actions, alarm)

		// Become candidate 
		if rsm.currState == "Follower" {
			rsm.currState = "Candidate"
		}

		// Update state variables
		rsm.currTerm++
		rsm.votedFor = rsm.selfId
		rsm.lastRepIndex = -1
		debug.Rsm("TOUT: F/C: 1", "id:", rsm.selfId, "term:", rsm.currTerm)

		// State store
		stateStore = StateStoreAction{rsm.currTerm, rsm.votedFor, rsm.lastRepIndex}
		actions = append(actions, stateStore)
		
		// Make Votesrcvd slice
		rsm.votesRcvd = make(map[int]int)
		for i = 0; i < len(rsm.peerIds); i++ {
			rsm.votesRcvd[rsm.peerIds[i]] = 0
		}

		// Ask for votes
		actions = append(actions, rsm.sendVoteRequestRPC(rsm.peerIds)...)

	// Leader behaviour
	case "Leader":

		// debug.Print(rsm.selfId, rsm.leaderId, rsm.currTerm, "tout: leader")
		debug.Rsm("TOUT: L: 1", "id:", rsm.selfId, "term:", rsm.currTerm)

		// Reset heartbeat alarm
		alarm = AlarmAction{rsm.heartbeatAlarmPeriod}
		actions = append(actions, alarm)		

		// Send heartbeat
		actions = append(actions, rsm.sendAppendEntriesRPC(rsm.peerIds)...)
	}
	return actions
}

// Append entries request event
func (rsm *RaftStateMachine) handleAppendEntriesReq(event Event) []Action {
	var (
		appendEntriesReq AppendEntriesReqEvent
		actions []Action
		appendEntriesResp AppendEntriesRespEvent
		stateStore StateStoreAction
		send SendAction
		commit CommitAction
		alarm AlarmAction
		logStore LogStoreAction
		i int
		senderId int
		senderTerm int
		prevLogIndex int64
		prevLogTerm int
		entries []LogEntry
		senderCommitIndex int64
	)

	// Initialization
	appendEntriesReq, _ = event.(AppendEntriesReqEvent)
	senderId = appendEntriesReq.SenderId
	senderTerm = appendEntriesReq.SenderTerm
	prevLogIndex = appendEntriesReq.PrevLogIndex
	prevLogTerm = appendEntriesReq.PrevLogTerm
	entries = appendEntriesReq.Entries
	senderCommitIndex = appendEntriesReq.SenderCommitIndex

	// debug.Print(rsm.selfId, rsm.leaderId, rsm.currTerm, "ar:", rsm.CommitIndex, senderCommitIndex)

	// Processing based on state
	switch rsm.currState {

	// All states have same response 
	case "Follower", "Candidate", "Leader":

		// Check whether the self has higher term
		if rsm.currTerm > senderTerm {

			// Send false response	.
			appendEntriesResp = AppendEntriesRespEvent{rsm.selfId, rsm.currTerm, rsm.lastRepIndex, false}
			send = SendAction{senderId, appendEntriesResp}
			actions = append(actions, send)

		// Sender is leader
		} else {

			// Reset election alarm
			alarm = AlarmAction{utils.RandRange(lb * rsm.electionAlarmPeriod, ub * rsm.electionAlarmPeriod)}
			actions = append(actions, alarm)

			// Become follower
			// Candidate becomes follower since it received append entries rpc with higher or same term
			// Leader becomes follower since it received append entries rpc with higher term. No possibility of two leaders in same term.
			if rsm.currState != "Follower" {	
				rsm.currState = "Follower"
			}

			// Update state variables if sender has higher term
			if rsm.currTerm < senderTerm {
				rsm.currTerm = senderTerm
				rsm.votedFor = -1
				rsm.lastRepIndex = -1

				// State store
				stateStore = StateStoreAction{rsm.currTerm, rsm.votedFor, rsm.lastRepIndex}
				actions = append(actions, stateStore)
			}

			// Set leaderid 
			rsm.leaderId = senderId

			// Check for non-matching previous index and term 
			if (prevLogIndex > rsm.lastLogIndex) || (prevLogIndex > -1 && (rsm.log[prevLogIndex].Term != prevLogTerm)) {

				// Send negative response
				appendEntriesResp = AppendEntriesRespEvent{rsm.selfId, rsm.currTerm, rsm.lastRepIndex, false}
				send = SendAction{senderId, appendEntriesResp}
				actions = append(actions, send)	
				debug.Rsm("AR: F/C/L: 1", "id:", rsm.selfId)

			// Check for reordered packets
			// Respond true only if the replication is already done for the sent request
			} else if rsm.lastRepIndex >= (prevLogIndex + int64(len(entries))) {

				// Updating commit index
				// Necessary since leader sends updated commit index only in the subsequent requests for entries replicated before
				if rsm.CommitIndex < rsm.lastLogIndex && rsm.CommitIndex < senderCommitIndex {
					debug.Rsm("AR: F/C/L: 4", "id:", rsm.selfId)
					rsm.CommitIndex = utils.Min(senderCommitIndex, rsm.lastLogIndex)

					// Commit action
					commit = CommitAction{rsm.leaderId, rsm.CommitIndex, rsm.log[rsm.CommitIndex], nil}
					actions = append(actions, commit)

					// debug.Print(rsm.selfId, rsm.leaderId, rsm.currTerm, "ar: new entry committed at", rsm.CommitIndex)
				}

				// Send postive response
				appendEntriesResp = AppendEntriesRespEvent{rsm.selfId, rsm.currTerm, rsm.lastRepIndex, true}
				send = SendAction{senderId, appendEntriesResp}
				actions = append(actions, send)
				debug.Rsm("AR: F/C/L: 2", "id:", rsm.selfId)

			// Previous index and term matches. New entries to be updated.
			} else {

				// Updating state variables
				rsm.lastLogIndex = prevLogIndex
				rsm.lastRepIndex = prevLogIndex
				rsm.log = rsm.log[:rsm.lastLogIndex + 1]

				// Updating log and associated variables
				for i = 0; i < len(entries); i++ {
					debug.Rsm("AR: F/C/L: 3", "id:", rsm.selfId)
					rsm.lastLogIndex++
					rsm.log = append(rsm.log, entries[i])

					// Log store
					logStore = LogStoreAction{rsm.lastLogIndex, entries[i]}
					actions = append(actions, logStore)

					rsm.lastLogTerm = rsm.log[rsm.lastLogIndex].Term
					rsm.lastRepIndex = rsm.lastLogIndex

					// debug.Print(rsm.selfId, rsm.leaderId, rsm.currTerm, "ar: new entry added at", rsm.lastLogIndex)					
				}

				// State store
				stateStore = StateStoreAction{rsm.currTerm, rsm.votedFor, rsm.lastRepIndex}
				actions = append(actions, stateStore)

				// Updating commit index
				if rsm.CommitIndex < rsm.lastLogIndex && rsm.CommitIndex < senderCommitIndex {
					debug.Rsm("AR: F/C/L: 4", "id:", rsm.selfId)
					rsm.CommitIndex = utils.Min(senderCommitIndex, rsm.lastLogIndex)

					// Commit action
					commit = CommitAction{rsm.leaderId, rsm.CommitIndex, rsm.log[rsm.CommitIndex], nil}
					actions = append(actions, commit)

					// debug.Print(rsm.selfId, rsm.leaderId, rsm.currTerm, "ar: new entry committed at", rsm.CommitIndex)	
				}

				// Send postive response
				appendEntriesResp = AppendEntriesRespEvent{rsm.selfId, rsm.currTerm, rsm.lastRepIndex, true}
				send = SendAction{senderId, appendEntriesResp}
				actions = append(actions, send)
			}
		}
	}
	return actions
}

// Append entries response event
func (rsm *RaftStateMachine) handleAppendEntriesResp(event Event) []Action {
	var (
		appendEntriesResp AppendEntriesRespEvent
		actions []Action
		alarm AlarmAction
		stateStore StateStoreAction
		commit CommitAction
		senderId int
		senderTerm int
		senderLastRepIndex int64
		response bool
		count int
		i int64
		j int
	)

	// Initialization
	appendEntriesResp, _ = event.(AppendEntriesRespEvent)
	senderId = appendEntriesResp.SenderId
	senderTerm = appendEntriesResp.SenderTerm
	senderLastRepIndex = appendEntriesResp.SenderLastRepIndex
	response = appendEntriesResp.Response

	// Processing based on state
	switch rsm.currState {

	// Follower and Candidate will never get append entries response
	case "Follower", "Candidate":
		// Ignore this event

	// Applicable only for leader
	case "Leader":

		// Check whether the sender is more updated.
		if rsm.currTerm < senderTerm {

			// Reset election alarm
			alarm = AlarmAction{utils.RandRange(lb * rsm.electionAlarmPeriod, ub * rsm.electionAlarmPeriod)}
			actions = append(actions, alarm)	

			// Become follower
			rsm.currState = "Follower"
			rsm.currTerm = senderTerm
			rsm.votedFor = -1
			rsm.lastRepIndex = -1

			// State store
			stateStore = StateStoreAction{rsm.currTerm, rsm.votedFor, rsm.lastRepIndex}
			actions = append(actions, stateStore)
			debug.Rsm("ARS: L: 1", "id:", rsm.selfId)	

		// Leader is more updated. 
		// Handle false response
		} else if response == false {

			// Update match index by using sender's last replicated info
			// Also consider packer reordering
			rsm.matchIndex[senderId] = utils.Max(rsm.matchIndex[senderId], senderLastRepIndex) 

			// Update next index
			rsm.nextIndex[senderId] = rsm.matchIndex[senderId] + 1

			// debug.Print(rsm.selfId, rsm.leaderId, rsm.currTerm, "ars:", senderId, senderLastRepIndex, rsm.nextIndex[senderId])

			// Retry append entries request
			actions = append(actions, rsm.sendAppendEntriesRPC([]int{senderId})...)
			debug.Rsm("ARS: L: 2", "id:", rsm.selfId)

		// Handle positive response
		} else if response == true {
			
			// Update match index. Handle Packer reordering
			rsm.matchIndex[senderId] = utils.Max(rsm.matchIndex[senderId], senderLastRepIndex) 

			// Update next index
			rsm.nextIndex[senderId] = rsm.matchIndex[senderId] + 1

			// Update commit index
			for i, count = rsm.matchIndex[senderId], 1; i > rsm.CommitIndex; i-- {
				for j = 0; j < len(rsm.peerIds); j++ {
					if rsm.matchIndex[rsm.peerIds[j]] >= i {
						count++
					}
				}
				debug.Rsm("ARS: L: 3", "id:", rsm.selfId, "count:", count, "senderid:", senderId, rsm.matchIndex)
				if count > rsm.clusterSize / 2 && rsm.log[i].Term == rsm.currTerm {	
					rsm.CommitIndex = i
					// debug.Print(rsm.selfId, rsm.leaderId, rsm.currTerm, "ars: leader commit:", rsm.CommitIndex)

					// Commit action
					commit = CommitAction{rsm.selfId, rsm.CommitIndex, rsm.log[rsm.CommitIndex], nil}
					actions = append(actions, commit)
					debug.Rsm("ARS: L: 4", "id:", rsm.selfId, "count:", count, "commitindex:", rsm.CommitIndex)
					break
				}
				count = 1
			}

			// Check for sending append entries rpc
			if rsm.lastLogIndex > rsm.matchIndex[senderId] {
				debug.Rsm("ARS: L: 5", "id:", rsm.selfId)		
				actions = append(actions, rsm.sendAppendEntriesRPC([]int{senderId})...)
			}
		}	
	}
	return actions
}

// Vote request event
func (rsm *RaftStateMachine) handleVoteReq(event Event) []Action {
	var (
		voteReq VoteReqEvent
		actions []Action
		alarm AlarmAction
		voteResp VoteRespEvent
		stateStore StateStoreAction
		send SendAction
		senderId int
		senderTerm int
		senderLastLogIndex int64
		senderLastLogTerm int
	)

	// Initialization
	voteReq, _ = event.(VoteReqEvent)
	senderId = voteReq.SenderId
	senderTerm = voteReq.SenderTerm
	senderLastLogIndex = voteReq.SenderLastLogIndex
	senderLastLogTerm = voteReq.SenderLastLogTerm

	// Processing based on state
	switch rsm.currState {

	// Main state for this event
	case "Follower":

		// Check if sender has a higher term
		if rsm.currTerm < senderTerm {
			rsm.currTerm = senderTerm
			rsm.votedFor = -1
			rsm.lastRepIndex = -1

			// State store
			stateStore = StateStoreAction{rsm.currTerm, rsm.votedFor, rsm.lastRepIndex}
			actions = append(actions, stateStore)		
			debug.Rsm("VR: F: 1", "id:", rsm.selfId)
		}

		// Check if self has a higher term
		if rsm.currTerm > senderTerm {

			// Send negative vote
			voteResp = VoteRespEvent{rsm.selfId, rsm.currTerm, false}
			send = SendAction{senderId, voteResp}
			actions = append(actions, send)
			debug.Rsm("VR: F: 2", "id:", rsm.selfId)

		// Sender term is either higher or equal to self term
		// Check if sender is less updated than self
		} else if (rsm.lastLogTerm > senderLastLogTerm) || (rsm.lastLogTerm == senderLastLogTerm && rsm.lastLogIndex > senderLastLogIndex) {

			// Send negative vote
			voteResp = VoteRespEvent{rsm.selfId, rsm.currTerm, false}
			send = SendAction{senderId, voteResp}
			actions = append(actions, send)
			debug.Rsm("VR: F: 3", "id:", rsm.selfId)

		// Sender is atleast updated as the self
		// Check if already voted
		} else if rsm.votedFor != -1 && rsm.votedFor != senderId {

			// Send negative vote
			voteResp = VoteRespEvent{rsm.selfId, rsm.currTerm, false}
			send = SendAction{senderId, voteResp}
			actions = append(actions, send)		
			debug.Rsm("VR: F: 4", "id:", rsm.selfId)

		// Sender is atleast as up-to-date as the self
		// Self not voted in this term. Give positive vote.
		}	else {

			// Reset election alarm
			alarm = AlarmAction{utils.RandRange(lb * rsm.electionAlarmPeriod, ub * rsm.electionAlarmPeriod)}
			actions = append(actions, alarm)			

			// Update vote variable
			rsm.votedFor = senderId

			// State store
			stateStore = StateStoreAction{rsm.currTerm, rsm.votedFor, rsm.lastRepIndex}
			actions = append(actions, stateStore)

			// Send positive vote
			voteResp = VoteRespEvent{rsm.selfId, rsm.currTerm, true}
			send = SendAction{senderId, voteResp}
			actions = append(actions, send)
			debug.Rsm("VR: F: 5", "id:", rsm.selfId)
		}

	// Same behaviour for both candidate and leader
	case "Candidate", "Leader":

		// Check if sender has higher term
		if rsm.currTerm < senderTerm {

			// Reset election alarm
			alarm = AlarmAction{utils.RandRange(lb * rsm.electionAlarmPeriod, ub * rsm.electionAlarmPeriod)}
			actions = append(actions, alarm)

			// Become follower
			rsm.currState = "Follower"
			rsm.currTerm = senderTerm
			rsm.votedFor = -1
			rsm.lastRepIndex = -1

			// State store
			stateStore = StateStoreAction{rsm.currTerm, rsm.votedFor, rsm.lastRepIndex}
			actions = append(actions, stateStore)				

			// Sender term is either higher or equal to self term
			// Check if sender is less updated than self			
			if (rsm.lastLogTerm > senderLastLogTerm) || (rsm.lastLogTerm == senderLastLogTerm && rsm.lastLogIndex > senderLastLogIndex) {

				// Send negative vote
				voteResp = VoteRespEvent{rsm.selfId, rsm.currTerm, false}
				send = SendAction{senderId, voteResp}
				actions = append(actions, send)
				debug.Rsm("VR: C/L: 1", "id:", rsm.selfId)

			// Sender is atleast as up-to-date as the self
			// Self not voted in this term. Give positive vote.
			} else {

				// Update vote variable
				rsm.votedFor = senderId

				// State store
				stateStore = StateStoreAction{rsm.currTerm, rsm.votedFor, rsm.lastRepIndex}
				actions = append(actions, stateStore)			

				// Send positive vote
				voteResp = VoteRespEvent{rsm.selfId, rsm.currTerm, true}
				send = SendAction{senderId, voteResp}
				actions = append(actions, send)
				debug.Rsm("VR: C/L: 7", "id:", rsm.selfId)
			}

		// Self has higher term than sender
		} else {

			// Send negative vote
			voteResp = VoteRespEvent{rsm.selfId, rsm.currTerm, false}
			send = SendAction{senderId, voteResp}
			actions = append(actions, send)
			debug.Rsm("VR: C/L: 8", "id:", rsm.selfId)
		}
	}
	return actions
}

// Vote response event
func (rsm *RaftStateMachine) handleVoteResp(event Event) []Action {
	var (
		voteResp VoteRespEvent
		actions []Action
		stateStore StateStoreAction
		alarm AlarmAction
		senderId int
		senderTerm int
		response bool
		numVotesGranted int
		numVotesDenied int
		i int
	)

	// Initialization
	voteResp, _ = event.(VoteRespEvent)
	senderId = voteResp.SenderId
	senderTerm = voteResp.SenderTerm
	response = voteResp.Response

	// Processing based on state
	switch rsm.currState {

	// Same behaviour for both follower and leader
	case "Follower", "Leader":

		// Check if sender has higher term
		if rsm.currTerm < senderTerm {

			// Become follower, if already was a leader
			if rsm.currState == "Leader" {

				// Reset election alarm
				alarm = AlarmAction{utils.RandRange(lb * rsm.electionAlarmPeriod, ub * rsm.electionAlarmPeriod)}
				actions = append(actions, alarm)	

				rsm.currState = "Follower"			
			}

			// Update state variables
			rsm.currTerm = senderTerm
			rsm.votedFor = -1
			rsm.lastRepIndex = -1

			// State store
			stateStore = StateStoreAction{rsm.currTerm, rsm.votedFor, rsm.lastRepIndex}
			actions = append(actions, stateStore)		
		}

	// Main state for this event
	case "Candidate":

		// Initialize required variables
		numVotesGranted = 1
		numVotesDenied = 0

		// Count positive and negative votes
		for i = 0; i < len(rsm.peerIds); i++ {
			if rsm.votesRcvd[rsm.peerIds[i]] == 1 {
				numVotesGranted++
			} else if rsm.votesRcvd[rsm.peerIds[i]] == -1 {
				numVotesDenied++
			}
		}

		// Check if sender has higher term
		if rsm.currTerm < senderTerm {

			// Reset election alarm
			alarm = AlarmAction{utils.RandRange(lb * rsm.electionAlarmPeriod, ub * rsm.electionAlarmPeriod)}
			actions = append(actions, alarm)	

			// Become follower
			rsm.currState = "Follower"		

			// Update state variables
			rsm.currTerm = senderTerm
			rsm.votedFor = -1
			rsm.lastRepIndex = -1

			// State store
			stateStore = StateStoreAction{rsm.currTerm, rsm.votedFor, rsm.lastRepIndex}
			actions = append(actions, stateStore)		

		// Check if self term is same as sender term
		}	else if rsm.currTerm == senderTerm {

				// Check for positive vote
				if response == true {

					// Update count for positive votes. Handle duplicate packets.
					if rsm.votesRcvd[senderId] == 0 {
						rsm.votesRcvd[senderId] = 1
						numVotesGranted++
					}

					debug.Rsm("VRS: C: 1", "id:", rsm.selfId, "numvotes:", numVotesGranted, rsm.votesRcvd)

					// Check if majority positive votes received
					if numVotesGranted > rsm.clusterSize / 2 {
						debug.Rsm("VRS: C: 2", "id:", rsm.selfId, "term:", rsm.currTerm, "in leader state now")

						// Reset heartbeat alarm
						alarm = AlarmAction{rsm.heartbeatAlarmPeriod}
						actions = append(actions, alarm)						

						// Become leader
						rsm.currState = "Leader"
						rsm.leaderId = rsm.selfId

						// Make leader specific variables
						rsm.nextIndex = make(map[int]int64)
						rsm.matchIndex = make(map[int]int64)
						for i = 0; i < len(rsm.peerIds); i++ {
							rsm.nextIndex[rsm.peerIds[i]] = rsm.lastLogIndex + 1
							rsm.matchIndex[rsm.peerIds[i]] = -1
						}

						// Send append RPC
						actions = append(actions, rsm.sendAppendEntriesRPC(rsm.peerIds)...)
					}

				// Check for negative vote
				} else if response == false {

					// Update count for negative vote. Handle duplicate packets.
					if rsm.votesRcvd[senderId] == 0 {
						rsm.votesRcvd[senderId] = -1
						numVotesDenied++
					}

					// Check if majoriy negative votes received
					if numVotesDenied > rsm.clusterSize / 2 {

						// Reset election alarm
						alarm = AlarmAction{utils.RandRange(lb * rsm.electionAlarmPeriod, ub * rsm.electionAlarmPeriod)}
						actions = append(actions, alarm)	

						// Become follower
						rsm.currState = "Follower"
					
						debug.Rsm("VRS: C: 3", "id:", rsm.selfId, "term:", rsm.currTerm, "in follower state now")
					}
				}		
		}
	}
	return actions
}