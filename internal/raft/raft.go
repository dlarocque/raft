package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"raft/internal/rpc"
	"sync"
	"sync/atomic"
	"time"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Index   int         // Index in the log
	Term    int         // Term when entry was received by the leader
	Command interface{} // Client command
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex       // Lock to protect shared access to this peer's state
	peers     []*rpc.ClientEnd // RPC end points of all peers
	persister *Persister       // Object to hold this peer's persisted state
	me        int              // this peer's index into peers[]
	dead      int32            // set by Kill()

	// Persistent state on all servers
	CurrentTerm int // Latest term server has seen
	VotedFor    int // CandidateId that received vote in current term
	Log         []LogEntry

	// Volatile state on all servers
	commitIndex   int       // Index of last log entry known to be committed
	lastApplied   int       // Index of last log entry known to be applied
	electionAlarm time.Time // Election timeout that starts an election
	state         state

	// Volatile state on leaders, re-initialized after elections
	nextIndex  []int // Next log index to send to each server
	matchIndex []int // Highest index known to be replicated on each server
}

// RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	Term         int // Candidate term
	CandidateId  int
	LastLogIndex int // Last index in the candidate's log
	LastLogTerm  int // Last term in the candidate's log
}

// RequestVote RPC reply structure.
type RequestVoteReply struct {
	Term        int  // Current term, in case the candidate needs to update itself
	VoteGranted bool // Received vote for election if true
}

type AppendEntriesArgs struct {
	Term         int        // Leader's term
	LeaderId     int        // Use so that followers can redirect clients
	PrevLogIndex int        // Index of log entry immediately preceeding new ones
	PrevLogTerm  int        // Term of log entry immediately preceeding new ones
	Entries      []LogEntry // Log entries to store
	LeaderCommit int        // Leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // Current term, in case the leader needs to update itself
	Success bool // True if follower contained an entry matching log entry and index
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.CurrentTerm, rf.state == Leader
}

// RequestVote RPC handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(rf, dVote, "Received RequestVote from S%d", args.CandidateId)

	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false

	// If RPC request contains term greater than our term, update our
	// current term, and convert to follower
	//
	// If we are currently a candidate running an election,
	// converting to a follower resets our vote for the new election term,
	// allowing us to grant the vote that we previously gave ourselves to
	// another candidate.
	if args.Term > rf.CurrentTerm {
		Debug(rf, dVote, "<- S%d has lower term (%d < %d), converting to follower", args.CandidateId, args.Term, rf.CurrentTerm)
		rf.convertToFollower(args.Term)
	}

	// Reply false if term < currentTerm
	if args.Term < rf.CurrentTerm || args.CandidateId == rf.me {
		Debug(rf, dVote, "<- S%d has a more recent term (%d < %d)", args.CandidateId, args.Term, rf.CurrentTerm)
		return
	}

	// If votedFor is null or candidateId, and candidate's log is at least as
	// up to date as receiver's log, grant a vote
	if rf.VotedFor == nullVote || rf.VotedFor == args.CandidateId {
		Debug(rf, dVote, "<- S%d sent vote", args.CandidateId)
		reply.VoteGranted = true
		rf.VotedFor = args.CandidateId
		// Reset election timer when we grand our vote to a peer
		Debug(rf, dVote, "<- S%d reset election time", args.CandidateId)
		rf.electionAlarm = initElectionAlarm()
	}
}

// goroutine
func (rf *Raft) requestVote(
	server int,
	args *RequestVoteArgs,
	pollingStation chan<- bool,
) {
	reply := &RequestVoteReply{}
	ok := rf.sendRequestVote(server, args, reply)
	vote := false
	if ok {
		rf.mu.Lock()

		// Our state may have been updated by an append entries, or request
		// vote, so we need to make sure our assumptions on state still hold.
		if rf.state == Candidate && rf.CurrentTerm == args.Term {
			// If we requested a vote from a server with a log that is more up
			// to date, we transition to the follower state, and we will update
			// our term to that logs latest term.
			if reply.Term > rf.CurrentTerm {
				Debug(rf, dTerm, "Requested Vote from S%d with %d (> T%d), converting to follower", server, reply.Term, rf.CurrentTerm)
				rf.convertToFollower(reply.Term)
			} else {
				vote = reply.VoteGranted
				if vote {
					Debug(rf, dVote, "Granted vote to S%d for T%d", args.CandidateId, args.Term)
				}
			}
		}

		// FIXME: After releasing this lock, could we receive an RequestVote RPC from a more up
		// to date candidate and return the follower state?
		rf.mu.Unlock()
	}

	pollingStation <- vote // Blocking until received
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(
	server int,
	args *RequestVoteArgs,
	reply *RequestVoteReply,
) bool {
	Debug(rf, dVote, "T%d -> S%d Sending Request Vote, LLI: %d, LLT: %d", args.Term, server, args.LastLogIndex, args.LastLogTerm)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) tallyVotes(term int, pollingStation <-chan bool) {
	voteCount := 1
	done := false

	Debug(rf, dVote, "Collecting votes from all peers")
	// Collect votes from all the peers
	for i := 0; i < len(rf.peers)-1; i++ {
		vote := <-pollingStation
		if vote {
			voteCount += 1
		}

		if !done && voteCount >= len(rf.peers)/2+1 {
			done = true
			rf.mu.Lock()

			Debug(rf, dVote, "VC: %d", voteCount)

			if rf.state != Candidate || rf.CurrentTerm != term {
				rf.mu.Unlock()
			} else {
				Debug(rf, dVote, "Transition to leader for T%d", rf.CurrentTerm)
				// If we have received votes from a majority of servers, become leader
				// Win the election
				// Update the volatile leader state
				rf.state = Leader

				lastLogEntry := rf.Log[len(rf.Log)-1]
				rf.nextIndex = make([]int, len(rf.peers))
				for i := range rf.nextIndex {
					rf.nextIndex[i] = lastLogEntry.Index + 1
				}

				rf.matchIndex = make([]int, len(rf.peers))
				for i := range rf.matchIndex {
					// We only know the last replicated log index on
					// our own server
					if i == rf.me {
						rf.matchIndex[i] = lastLogEntry.Index
					} else {
						rf.matchIndex[i] = 0
					}
				}

				term := rf.CurrentTerm
				rf.mu.Unlock()
				// Upon election, send heartbeats to each server to prevent election timeouts,
				// and repeat during idle periods
				go rf.pacemaker(term)
				// There is no need to convert to follower, if we have lost the
				// election, and the leader will convert us to the follower state
				// by sending a heartbeat.

			}
		}
	}
}

// Returns whether the log with lastLogIndex and lastLogTerm is more up to
// date than our log.
// Assumes the mutex is held.
func (rf *Raft) logIsMoreUpToDate(lastLogIndex, lastLogTerm int) bool {
	// The other log is more up to date if its last log entry term is greater
	// than our last log entry term.
	lastLogEntry := rf.Log[len(rf.Log)-1]
	if lastLogEntry.Term > lastLogTerm {
		return false
	} else if lastLogEntry.Term < lastLogTerm {
		return true
	}

	// If the last log entry terms are equal, the last log entry
	// with the highest index belongs to the log that is the most up to date
	return lastLogEntry.Index <= lastLogIndex

}

// Transition to the follower state, and enter the most recent term.
func (rf *Raft) convertToFollower(term int) {
	rf.CurrentTerm = term
	rf.state = Follower
	rf.VotedFor = nullVote
	rf.nextIndex = nil
	rf.matchIndex = nil
	Debug(rf, dTerm, "converted to follower for T%d", term)
}

// Periodically send heartbeats to all peers
func (rf *Raft) pacemaker(term int) {
	for !rf.killed() { // Make sure this doesn't keep running
		rf.mu.Lock()

		// Validate that we are still the leader for the current term
		if rf.state == Leader && rf.CurrentTerm == term {
			// Send heartbeats to all peers to reset their election timers
			rf.mu.Unlock()
			Debug(rf, dLeader, "pacemaker sending heartbeats, T%d", term)
			for i := range rf.peers {
				if i != rf.me {
					go rf.appendEntries(i, term)
				}
			}

			// Tests restrict sending heartbeats more than 10 times per second
			Debug(rf, dTimer, "pacemaker going to sleep for %dms", 100)
			time.Sleep(100 * time.Millisecond)
		} else {
			Debug(rf, dLeader, "pacemaker stopped for T%d", term)
			rf.mu.Unlock()
			return
		}
	}
}

// goroutine
func (rf *Raft) appendEntries(server int, term int) {
	// Construct request
	rf.mu.Lock()
	// Validate state
	if rf.state != Leader || rf.CurrentTerm != term {
		rf.mu.Unlock()
		return
	}
	prevLogIndex := rf.nextIndex[server] - 1
	args := &AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.Log[prevLogIndex].Term,
		Entries:      []LogEntry{},
		LeaderCommit: 0, // TODO
	}
	rf.mu.Unlock()

	reply := &RequestVoteReply{}
	ok := rf.sendAppendEntries(server, args, reply)
	if ok {
		rf.mu.Lock()

		if reply.Term > rf.CurrentTerm {
			Debug(rf, dTerm, "<- S%d at T%d is more up to date", server, reply.Term)
			rf.convertToFollower(reply.Term)
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) sendAppendEntries(
	server int,
	args *AppendEntriesArgs,
	reply *RequestVoteReply,
) bool {
	Debug(rf, dLog2, "-> S%d, Sending Heartbeat for T%d", server, args.Term)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Append Entries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(rf, dLog, "<- S%d: T%d Received AppendEntries", args.LeaderId, args.Term)

	reply.Term = rf.CurrentTerm
	reply.Success = false

	if args.LeaderId == rf.me {
		panic("Leader sent AppendEntries to self")
	}

	// 1. Reply false if term < currentTerm
	if args.Term < rf.CurrentTerm {
		Debug(rf, dLog, "<- S%d: T%d < T%d, replying false since we are more up to date", args.LeaderId, args.Term, rf.CurrentTerm)
		return
	}

	// TODO: 2. Reply false if log doesn't contain an entry at prevLogIndex whose term
	// matches prevLogTerm

	// TODO: 3. If an existing entry conflicts with a new one, delete the existing entry and
	// all that follow it.

	// TODO: 4. Append any new entries not already in the log.

	// TODO: 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

	// If Append Entries received from server with term greater than ours, convert to follower.
	if args.Term > rf.CurrentTerm {
		Debug(rf, dLog, "T%d < T%d, converting to follower for T%d", args.LeaderId, args.Term, rf.CurrentTerm, args.Term)
		rf.convertToFollower(args.Term)
	}

	// Since this AppendEntries RPC comes from the current leader, we want to reset our
	// election alarm.
	Debug(rf, dTimer, "Resetting ELA for T%d", args.Term)
	rf.electionAlarm = initElectionAlarm()
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	var sleepDuration time.Duration
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state == Follower || rf.state == Candidate {
			Debug(rf, dTimer, "Follower checking election timeout")
			// Check if a leader election should be started
			if rf.electionAlarm.After(time.Now()) {
				sleepDuration = time.Until(rf.electionAlarm)
				rf.mu.Unlock()
			} else {
				Debug(rf, dTimer, "Election timeout has expired, follower converting to candidate, calling election for T%d", rf.CurrentTerm+1)
				// Convert to candidate
				// On conversion to candidate, start an election:
				// 1. Increment current term
				rf.CurrentTerm += 1
				rf.state = Candidate

				// 2. Vote for self
				rf.VotedFor = rf.me

				// 3. Reset election timer
				rf.electionAlarm = initElectionAlarm()
				sleepDuration = time.Until(rf.electionAlarm)

				// 4. Send RequestVote RPCs to all other servers
				args := &RequestVoteArgs{
					Term:         rf.CurrentTerm,
					CandidateId:  rf.me,
					LastLogIndex: rf.Log[len(rf.Log)-1].Index,
					LastLogTerm:  rf.Log[len(rf.Log)-1].Term,
				}

				rf.mu.Unlock()

				pollingStation := make(chan bool)
				for server := range rf.peers {
					if server != rf.me {
						go rf.requestVote(server, args, pollingStation)
					}
				}

				go rf.tallyVotes(args.Term, pollingStation)
			}
		} else if rf.state == Leader {
			rf.electionAlarm = initElectionAlarm()
			sleepDuration = time.Until(rf.electionAlarm)
			rf.mu.Unlock()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		Debug(rf, dTimer, "Ticker going to sleep for %dms", sleepDuration.Milliseconds())
		time.Sleep(sleepDuration)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(
	peers []*rpc.ClientEnd,
	me int,
	persister *Persister,
	applyCh chan ApplyMsg,
) *Raft {
	rf := &Raft{
		peers:     peers,
		me:        me,
		persister: persister,
		dead:      0,

		CurrentTerm: 0,
		VotedFor:    nullVote,
		Log:         []LogEntry{{Index: 0, Term: 0}},

		commitIndex:   0,
		lastApplied:   0,
		state:         Follower,
		electionAlarm: initElectionAlarm(),

		nextIndex:  nil,
		matchIndex: nil,
	}

	Debug(rf, dClient, "Started at T%d, ET %d", rf.CurrentTerm, rf.electionAlarm.UnixMilli()-time.Now().UnixMilli())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) committer() {

}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}
