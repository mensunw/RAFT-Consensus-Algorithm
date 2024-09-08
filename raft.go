package raft

//
// This is an outline of the API that raft must expose to
// the service (or tester). See comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   Create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   Start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester) in the same server.
//

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"cs350/labgob"
	"cs350/labrpc"
)

// struct for each log entry
type logEntrae struct {
	Term    int
	Command interface{}
}

// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). Set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // This peer's index into peers[]
	dead      int32               // Set by Kill()

	// Your data here (4A, 4B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int         // latest term server has seen
	votedFor    int         // candidateId that received vote in current term, null = none
	log         []logEntrae // list of log entries

	heartbeatChan chan int  // channel for tracking heartbeats, 1 = heartbeat
	state         int       // follower = 0, candidate = 1, leader = 2
	votes         int       // stores votes
	wonElecChan   chan bool // channel for the candidate's election win

	commitIndex int   // highset index known to be committed for log entries
	nextIndex   []int // for each server, index of next log entry to send
	matchIndex  []int // for each server, index of highest log entry known to be replicated

	applyCh     chan ApplyMsg // a global channel for the leader to commit entries
	lastApplied int           // index of highest log entry applied to state machine
}

// min function
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (4A).

	//fmt.Println("CHECKING GET STATE " + fmt.Sprint(rf.me))
	rf.mu.Lock()
	//fmt.Println(fmt.Sprint(rf.me) + " got lock")
	term = rf.currentTerm
	if rf.state == 2 {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()

	return term, isleader
}

// Save Raft's persistent state to stable storage, where it
// can later be retrieved after a crash and restart. See paper's
// Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (4B).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// Restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	// Your code here (4B).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []logEntrae
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		panic("error read persist")
	} else {
		//fmt.Println("CURRENT votedFor FROM READPERSIST: " + fmt.Sprint(votedFor))
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}

}

// Example RequestVote RPC arguments structure.
// Field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (4A, 4B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// Example RequestVote RPC reply structure.
// Field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (4A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// Example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (4A, 4B).

	// Other server handles the RPC made from candidate with this

	//fmt.Println("term: " + fmt.Sprint(args.Term))

	//fmt.Println("currnt term: " + fmt.Sprint(rf.currentTerm))

	//fmt.Println("GOT VOTE REQ")

	rf.mu.Lock()
	//fmt.Println("candidate's term: " + fmt.Sprint(args.Term))
	//fmt.Println(fmt.Sprint(rf.me) + "'s currentTerm: " + fmt.Sprint(rf.currentTerm))
	// if candidate term is > server's term, give them vote ONLY IF log is updated
	if args.Term > rf.currentTerm {

		// first determine if candidate log is at least up to date as this server's log
		atLeastUpToDate := true
		// check if terms are not equal
		serverLastLogTerm := rf.log[len(rf.log)-1].Term
		if serverLastLogTerm != args.LastLogTerm {
			// if not equal, then greater term is more up-to-date
			if serverLastLogTerm > args.LastLogTerm {
				atLeastUpToDate = false
			}
		} else {
			// if equal, then longer log is more up-to-date
			if len(rf.log)-1 > args.LastLogIndex {
				atLeastUpToDate = false
			} else if len(rf.log)-1 == args.LastLogIndex {
				//fmt.Println("equal term and equal index case, it's still up to date! ")
				// equal term and equal index case, it's still up to date!
			}
		}
		//fmt.Println(args.Term)
		//fmt.Println(args.LastLogTerm)

		if atLeastUpToDate {
			// now grant vote
			//fmt.Println("Method 1: " + fmt.Sprint(rf.me) + " has granted vote to " + fmt.Sprint(args.CandidateId))
			reply.VoteGranted = true
			reply.Term = -1

			// set currentTerm = term, convert to follower
			rf.currentTerm = args.Term
			rf.state = 0
			rf.votedFor = args.CandidateId
			rf.persist()
			rf.mu.Unlock()

			// reset timer for granting a vote
			// TMP change to append entry RPC call
			//rf.heartbeatChan <- 1
			go rf.sendHeartBeat()
			//fmt.Println("AFTER sending heartbeat from granting vote method 1")
		} else {
			// don't grant vote, but convert to follower bc term is lower
			reply.VoteGranted = false
			reply.Term = -1
			rf.state = 0
			rf.currentTerm = args.Term
			rf.persist()
			rf.mu.Unlock()
		}

	} else if args.Term < rf.currentTerm {
		// reply false if candidate's term is < server's term

		// this will cause candidate to become a follower
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
	} else {

		// first determine if candidate log is at least up to date as this server's log
		atLeastUpToDate := true
		// check if terms are not equal
		serverLastLogTerm := rf.log[len(rf.log)-1].Term
		if serverLastLogTerm != args.LastLogTerm {
			// if not equal, then greater term is more up-to-date
			if serverLastLogTerm > args.LastLogTerm {
				atLeastUpToDate = false
			}
		} else {
			// if equal, then longer log is more up-to-date
			if len(rf.log)-1 > args.LastLogIndex {
				atLeastUpToDate = false
			} else if len(rf.log)-1 == args.LastLogIndex {
				//fmt.Println("equal term and equal index case, it's still up to date! ")
				// equal term and equal index case, it's still up to date!
			}
		}

		// (if this server's vote is null or has voted for candidate (?)) AND log is at least up to date to this server's log
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && atLeastUpToDate {
			//fmt.Println("Method 2: " + fmt.Sprint(rf.me) + " has granted vote to " + fmt.Sprint(args.CandidateId))
			reply.VoteGranted = true
			reply.Term = -1

			// set currentTerm = term, convert to follower
			rf.currentTerm = args.Term
			rf.state = 0
			rf.votedFor = args.CandidateId
			rf.persist()
			rf.mu.Unlock()

			// reset timer for granting a vote
			// TMP change to append entry RPC call
			//rf.heartbeatChan <- 1
			go rf.sendHeartBeat()

		} else {
			//fmt.Println("voted for status: " + fmt.Sprint(rf.votedFor)) // should be -1
			reply.VoteGranted = false
			reply.Term = -1
			rf.mu.Unlock()
		}

	}

}

type AppendEntriesArgs struct {
	Term         int         // leader's term
	LeaderId     int         // for followers to redirect clients
	PrevLogIndex int         // index of previous log entry of new one
	PrevLogTerm  int         // term of the previous log entry of new one
	Entries      []logEntrae // log entries to store, empty for heartbeats
	LeaderCommit int         // leader's commitIndex
}

type AppendEntriesReply struct {
	Term          int  // currentTerm, for leader to update itself
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm & term is correct
	ConflictIndex int
	ConflictTerm  int
}

// append entries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	// deny heartbeat and appendentries if leader's term is < server's term, otherwise accept heartbeat if empty entries
	rf.mu.Lock()

	//fmt.Println("Server " + fmt.Sprint(rf.me) + ": rf.log-> " + fmt.Sprint(rf.log))
	//fmt.Println("Server " + fmt.Sprint(rf.me) + ": args.PrevLogIndex-> " + fmt.Sprint(args.PrevLogIndex))
	fmt.Println("Server " + fmt.Sprint(rf.me) + ": len(rf.log)-1-> " + fmt.Sprint(len(rf.log)-1))
	//fmt.Println("Server " + fmt.Sprint(rf.me) + ": args.Term-> " + fmt.Sprint(len(rf.log)-1))

	if args.Term < rf.currentTerm {
		// return correct term & convert leader to follower
		//fmt.Println("!!! Leader has lower term than follower here")
		//fmt.Println(args.Term)
		//fmt.Println(rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
	} else {

		// if no prevLogIndex in log ...
		if !(len(rf.log) > args.PrevLogIndex) {
			// should return conflictIndex = len(log) & conflictTerm = None
			reply.ConflictIndex = len(rf.log)
			reply.ConflictTerm = -2 // None
		}

		// if prevLogIndex is in log but term does not match ...
		if len(rf.log) > args.PrevLogIndex && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			// return conflictTerm = log[prevLogIndex].Term
			conflictTerm := rf.log[args.PrevLogIndex].Term
			reply.ConflictTerm = conflictTerm

			// then search its log for the first index whose entry has term equal to conflictTerm
			index := 0
			for rf.log[index].Term != conflictTerm {
				index++
			}
			reply.ConflictIndex = index
		}

		// if commitIndex > lastApplied, then increment lastAppliedand apply to state machine
		for rf.commitIndex > rf.lastApplied {
			//if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			// apply to state machine
			newMsg := ApplyMsg{true, rf.log[rf.lastApplied].Command, rf.lastApplied}
			fmt.Println("Server " + fmt.Sprint(rf.me) + ": Applying to state machine " + fmt.Sprint(newMsg))
			//fmt.Println("NOW commitIndex: " + fmt.Sprint(rf.commitIndex))
			// unlock before ch
			//rf.mu.Unlock()
			// create go routine just incase not constant draining on other side
			//go func(s ApplyMsg) {
			rf.applyCh <- newMsg //rf.applyCh <- s
			//}(newMsg)

			//}
		}

		// note: if append entries term > then currentterm, then it must convert to follower
		if args.Term > rf.currentTerm {
			rf.state = 0
		}

		// check if any existing log entries conflict with args.entries
		for index, element := range args.Entries {
			// check if there is an entry there first
			if len(rf.log)-1 >= args.PrevLogIndex+1+index {
				currLogEntry := rf.log[args.PrevLogIndex+1+index]
				if currLogEntry.Term != element.Term {
					rf.log = rf.log[:args.PrevLogIndex+1+index]
					rf.persist()
					//fmt.Println(fmt.Sprint(rf.me) + " is DELETING ENTRIES!")
				} else {
					//fmt.Println(fmt.Sprint(rf.me) + ", an entry aleady exists, should be same")
				}
			}
		}

		if !(len(rf.log) > args.PrevLogIndex && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm) {
			// return false if log does not contain an entry at prevLogIndex whose term matches prevLogTerm
			// !!! maybe add more here later
			// check if there is an existing entry
			if len(rf.log) > args.PrevLogIndex {
				// check if existing entry conflicts with new one (same index but diff terms)
				if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
					// delete existing entry and all that follow it
					rf.log = rf.log[:args.PrevLogIndex]
					rf.persist()
				} else {
					// note: should be going there EVERY time, shouldnt be here
					fmt.Println("something wrong: appendentries")
				}
			}
			reply.Term = -1
			reply.Success = false

			// check if empty appendentries for heartbeat reasons
			if (len(args.Entries)) == 0 {
				//rf.heartbeatChan <- 1
				go rf.sendHeartBeat()
			}
			rf.mu.Unlock()
			return

		} else {
			// appendentries either heartbeat or not, follower contains entry matching prevLogIndex & prevLogTerm
			//fmt.Println(fmt.Sprint(rf.me) + ": returning true for reply")
			reply.Success = true
			reply.Term = -1

			//fmt.Println("Server " + fmt.Sprint(rf.me) + ": args.LeaderCommit: " + fmt.Sprint(args.LeaderCommit))
			//fmt.Println("Server " + fmt.Sprint(rf.me) + ": rf.commitIndex: " + fmt.Sprint(rf.commitIndex))
			if len(args.Entries) == 0 {
				// empty entries means heartbeat
				//fmt.Println(fmt.Sprint(rf.me) + " got append entries req")
				// note: if hearbeat term > then currentterm, then it must convert to follower
				rf.state = 0

				// if leader's commit index > commitIndex, set commitIndex to min(leaderCommit, index of last new entry)
				// note: dont think need this here

				if args.LeaderCommit > rf.commitIndex {
					rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
					// also change lastApplied, as this one of few places where commitIndex can change

					// since commitIndex just updated, keep applying to state machine

					for rf.commitIndex > rf.lastApplied {
						rf.lastApplied++
						// apply to state machine
						newMsg := ApplyMsg{true, rf.log[rf.lastApplied].Command, rf.lastApplied}
						//fmt.Println("Server " + fmt.Sprint(rf.me) + ": Applying to state machine " + fmt.Sprint(newMsg))
						rf.mu.Unlock()
						rf.applyCh <- newMsg
						rf.mu.Lock()
					}
					rf.mu.Unlock()
					// check if empty appendentries (given here) for heartbeat reasons
					//rf.heartbeatChan <- 1
					go rf.sendHeartBeat()
				} else {
					rf.mu.Unlock()
					//rf.heartbeatChan <- 1
					go rf.sendHeartBeat()
				}
				return

			} else {
				//fmt.Println("Server " + fmt.Sprint(rf.me) + ": entry matched! (not heartbeat)")
				// append the entries to log
				// MAKE SURE entries are new

				for index, element := range args.Entries {
					// check if there is an entry there first
					if len(rf.log)-1 >= args.PrevLogIndex+1+index {
						if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
							// error?
							fmt.Println("APPENDING ERROR HERE????")
						} else {
							// do nothing, do not append or else it'll be the same
						}
					} else {
						// entry here does not exist, so append
						rf.log = append(rf.log, element)
						rf.persist()
					}
				}
				//fmt.Println("Server " + fmt.Sprint(rf.me) + ": new log append, now log is: " + fmt.Sprint(rf.log))

				// if leader's commit index > commitIndex, set commitIndex to min(leaderCommit, index of last new entry)
				if args.LeaderCommit > rf.commitIndex {
					rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)

					// since commitIndex just updated, keep applying to state machine

					for rf.commitIndex > rf.lastApplied {
						rf.lastApplied++
						// apply to state machine
						newMsg := ApplyMsg{true, rf.log[rf.lastApplied].Command, rf.lastApplied}
						//fmt.Println("Server " + fmt.Sprint(rf.me) + ": Applying to state machine " + fmt.Sprint(newMsg))
						rf.mu.Unlock()
						rf.applyCh <- newMsg
						rf.mu.Lock()
					}
					rf.mu.Unlock()
				} else {
					//fmt.Println(rf.log)
					rf.mu.Unlock()
				}
				return
			}
		}

	}
}

// Example code to send a RequestVote RPC to a server.
// Server is the index of the target server in rf.peers[].
// Expects RPC arguments in args. Fills in *reply with RPC reply,
// so caller should pass &reply.
//
// The types of the args and reply passed to Call() must be
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
// Look at the comments in ../labrpc/labrpc.go for more details.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) error {
	//fmt.Println(fmt.Sprint(args.CandidateId) + " sending vote req to: " + fmt.Sprint(server))
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	// send status into RPC vote request collector channel
	//rf.RPCVoteChan <- VoteStatus{ok, reply.VoteGranted, reply.Term}
	//fmt.Println("SENT THE RPCVOTECHAN")

	if ok {
		// if req vote RPC is success then...
		rf.mu.Lock()

		// make sure not already leader
		if rf.state != 2 {

			// if server's term is > candidate's term is, set candidate's term to server's and convert to follower
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = 0
				rf.persist()
			} else {

				// for each RPC vote request, see if they voted for the candidate
				if reply.VoteGranted {
					rf.votes += 1
					//fmt.Println("votes: " + fmt.Sprint(rf.votes))
				}

				// check WITHIN loop to see if there is majority & still candidacy
				maj := math.Ceil(float64((len(rf.peers) + 1)) / 2.0)
				if rf.state == 1 && rf.votes >= int(maj) { // CHANGE THIS 2nd one, hardcoded for now
					// won the election!
					// use print here to test, only 1 can win this
					//fmt.Println(fmt.Sprint(rf.me) + " won the election!")
					rf.state = 2
					rf.wonElecChan <- true

					// re-init nextIndex
					rf.nextIndex = make([]int, len(rf.peers))
					for server := 0; server < len(rf.peers); server++ {
						// set to index of leader's last log + 1
						rf.nextIndex[server] = len(rf.log)
					}
					// re-init matchIndex
					rf.matchIndex = make([]int, len(rf.peers))
					for server := 0; server < len(rf.peers); server++ {
						// set to index of leader's last log + 1
						rf.matchIndex[server] = 0
					}
				}
			}
		}
		//fmt.Println("TESTINGS")
		rf.mu.Unlock()

	} else {
		//fmt.Println("RPC req vote failed")
	}

	// return nothing
	return nil
}

// append entries RPC sender
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	//fmt.Println(fmt.Sprint(server) + " got append entries req")

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if ok {

		rf.mu.Lock()
		// check if other server agreed with the heartbeat, make sure still leader here (these operations are only for the LEADER)
		if !reply.Success && rf.state == 2 {
			// if not then that then maybe term incorrect
			if reply.Term > rf.currentTerm {
				// convert to follower
				rf.currentTerm = reply.Term
				//fmt.Println("STATE CHANGED FOR LEADER bc appendentry returned higher term")
				rf.state = 0
				rf.persist()
				rf.mu.Unlock()
				return nil
			} else {
				//fmt.Println("log inconsis for: " + fmt.Sprint(server))
				//fmt.Println("nextIndex for server " + fmt.Sprint(server) + ": " + fmt.Sprint(rf.nextIndex[server]))
				//fmt.Println("prevLogIndex for server "+fmt.Sprint(server)+": ", args.PrevLogIndex)
				// log inconsistency must have led to here (a failure return)
				// decrement nextIndex and retry

				rf.nextIndex[server] = rf.nextIndex[server] - 1

				// last log index >= nextIndex for a follower
				if args.PrevLogIndex >= rf.nextIndex[server] {
					// (re)send append entries RPC with log entries starting at nextIndex

					// Upon receiving a conflict response, the leader should first search its log for conflictTerm
					found := false // track to see if found it or not
					for index := len(rf.log) - 1; index >= 0; index-- {
						//fmt.Println(rf.log)
						// If it finds an entry in its log with that term, it should set nextIndex to be the one beyond the index of the last entry in that term in its log
						if rf.log[index].Term == reply.ConflictTerm {
							rf.nextIndex[server] = index + 1
							found = true
							break
						}
					}

					// if it does not find an entry with that term, it should set nextIndex = conflictIndex
					if !found {
						rf.nextIndex[server] = reply.ConflictIndex
					}

					args.Entries = rf.log[rf.nextIndex[server]:]

					// re configure the PrevLogIndex
					if rf.nextIndex[server]-1 <= -1 {
						args.PrevLogIndex = 0
					} else {
						args.PrevLogIndex = rf.nextIndex[server] - 1 //args.PrevLogIndex - 1
					}

					// re configure PrevLogTerm to match the PrevLogIndex
					args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
					//fmt.Println(rf.nextIndex[server])
					//fmt.Println(rf.log)
					//fmt.Println("Sending this entry back to " + fmt.Sprint(server) + ": " + fmt.Sprint(args.Entries))
					//fmt.Println("THIS IS INSIDE: " + fmt.Sprint(rf.nextIndex[server]+1) + "")
					// go routine here
					go rf.sendAppendEntries(server, args, reply)
				} else {
					// couldnt retry bc sudden nextIndex switch, just put it back then
					rf.nextIndex[server] = rf.nextIndex[server] + 1
					//fmt.Println(fmt.Sprint(server) + " just REACHED HERE??? WHAT IS THIS?")
				}
			}
		} else if reply.Success && rf.state == 2 {
			// if success then follower has matching entry and must have appending entry to make identical (out of the RPC "loop" now)
			// PRINT HERE to check for requirement of identical logs maybe, note: might not be identical tho bc leader could have added new command midway

			// ! check if entries is empty, bc if it is then it's a heartbeat, dont do these steps
			if len(args.Entries) != 0 {
				// set nextIndex to current length of leader's entries
				//fmt.Println("replied success and appendentries not empty, server", server)
				//LeaderEntries := len(args.Entries)+1// supposed to be len(rf.log)
				//fmt.Println("nextIndex currently: " + fmt.Sprint(rf.nextIndex[server]))
				//fmt.Println("what we want for nextIndex most of time: " + fmt.Sprint(len(rf.log)))
				//fmt.Println("length of entries: " + fmt.Sprint(len(args.Entries)))
				rf.nextIndex[server] = len(rf.log)
				//fmt.Println("CHANGED NEXTINDEX FOR SERVER " + fmt.Sprint(server) + ": " + fmt.Sprint(rf.nextIndex[server]))

				// set matchIndex to index of highest log entry known to be replicated on server
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries) // not len(rf.log) - 1

			}
			//see if majority of matchIndexes also have >= N
			maj := math.Ceil(float64((len(rf.peers) + 1)) / 2.0)

			for n := 0; n < len(rf.log); n++ {

				// make sure it's greater than current leader's commitIndex
				if n > rf.commitIndex {
					count := 0
					for server := 0; server < len(rf.peers); server++ {
						//fmt.Println("matchinex, server: ", rf.matchIndex[server], server)
						if rf.matchIndex[server] >= n {
							count++
						}
					}
					// if they do, and term is equal to currentTerm, then update leader's commitIndex
					if count >= int(maj) && rf.log[n].Term == rf.currentTerm {
						rf.commitIndex = n
						//fmt.Println("Updated leader's commitIndex to: ", n)
					}
				} // else continue to next n

			}
			// if commit changes and is > than lastApplied then...
			for rf.commitIndex > rf.lastApplied {
				rf.lastApplied++
				// apply to state machine
				newMsg := ApplyMsg{true, rf.log[rf.lastApplied].Command, rf.lastApplied}
				//fmt.Println("(Leader) Server " + fmt.Sprint(rf.me) + ": Applying to state machine " + fmt.Sprint(newMsg))
				//fmt.Println("log length: " + fmt.Sprint(len(rf.log)))
				// unlock before ch
				rf.mu.Unlock()
				rf.applyCh <- newMsg
				rf.mu.Lock()

				//go func(s ApplyMsg) {
				//rf.applyCh <- s
				//}(newMsg)

			}

		}

		rf.mu.Unlock()
	} else {
		//fmt.Println("RPC failed")
	}

	// return nothing
	return nil
}

// function for sending a heartbeat (use go routine for this)
func (rf *Raft) sendHeartBeat() {
	//fmt.Println("SENT HEARTBEAT")
	rf.heartbeatChan <- 1
	//fmt.Println("RECEIVED A HEARTBEAT")
}

// function for starting election
func (rf *Raft) startElection() {
	// check for deadlock later print here
	rf.mu.Lock()

	//fmt.Println(fmt.Sprint(rf.me) + " has timed out, so they started an election!")

	// set state to candidate
	rf.state = 1

	// increase own term and vote for itself
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.votes = 1 // vote for itself, maybe even restart the vote count
	rf.persist()
	rf.mu.Unlock()

	// send out request vote RPCS to other servers
	for server := 0; server < len(rf.peers); server++ {
		// don't send req to itself
		if server != rf.me {
			// give args the current term and current server candidate Id

			rf.mu.Lock()
			lastLogIndex := len(rf.log) - 1
			lastLogTerm := rf.log[lastLogIndex].Term
			args := RequestVoteArgs{rf.currentTerm, rf.me, lastLogIndex, lastLogTerm}
			reply := RequestVoteReply{}
			rf.mu.Unlock()

			// send out req instantly for each one
			go rf.sendRequestVote(server, &args, &reply)

		}
	}
}

// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. If this
// server isn't the leader, returns false. Otherwise start the
// agreement and return immediately. There is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. Even if the Raft instance has been killed,
// this function should return gracefully.
//
// The first return value is the index that the command will appear at
// if it's ever committed. The second return value is the current
// term. The third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (4B).

	rf.mu.Lock()
	// check if leader
	if rf.state == 2 {
		isLeader = true
		// append entry to local log
		newEntry := logEntrae{rf.currentTerm, command}
		rf.log = append(rf.log, newEntry)
		rf.persist()
		rf.matchIndex[rf.me] = len(rf.log) - 1
		// start the agreement within heartbeats, no need here but will send anyawys
		//fmt.Println(len(rf.log))
		//fmt.Println("Leader's log after start command: " + fmt.Sprint(rf.log))
		for server := 0; server < len(rf.peers); server++ {
			if server != rf.me { // don't update self
				rf.nextIndex[server] += 1 // INCREASE the nextIndex for this one entry

				prevLogIndex := len(rf.log) - 1
				prevLogTerm := rf.log[prevLogIndex].Term
				entries := []logEntrae{} // empty for first
				leaderCommit := rf.commitIndex

				//fmt.Println("Server " + fmt.Sprint(rf.me) + ", AppendEntries to " + fmt.Sprint(server) + ": " + "rf.currentTerm-> " + fmt.Sprint(rf.currentTerm) + ", " + "rf.me-> " + fmt.Sprint(rf.me) + ", " + "prevLogIndex-> " + fmt.Sprint(prevLogIndex) + ", " + "prevLogTerm-> " + fmt.Sprint(prevLogTerm) + ", " + "entries-> " + fmt.Sprint(entries) + ", " + "rf.leaderCommit-> " + fmt.Sprint(leaderCommit))
				args := AppendEntriesArgs{rf.currentTerm, rf.me, prevLogIndex, prevLogTerm, entries, leaderCommit} // args for append entries RPC
				reply := AppendEntriesReply{}

				// send out append entries heartbeats instantly for each one
				go rf.sendAppendEntries(server, &args, &reply)

			}
		}

		// index of log entry if committed
		index = len(rf.log) - 1
	}

	// set term
	term = rf.currentTerm

	rf.mu.Unlock()

	return index, term, isLeader
}

// The tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. Your code can use killed() to
// check whether Kill() has been called. The use of atomic avoids the
// need for a lock.
//
// The issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. Any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	//fmt.Println("KILLED")
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		//find a way to check if this peer has not recieved heartbeats before timer is up

		// !!! sleep certain amoutn of random time here
		// check if recieved heartbeat RPC

		// 500 to 1000 miliseconds
		randomTimeout := 0.5 + rand.Float64()*0.5
		//fmt.Println("randomtimeout: " + fmt.Sprint(randomTimeout))

		rf.mu.Lock()
		if rf.state == 0 {
			rf.mu.Unlock()

			// server is a follower, 2 cases:

			select {
			// case 1: recieved heartbeat from leader or from voting for a candidate
			case <-rf.heartbeatChan:
				// there's a heart beat before timer expired!, so restart random timer by doing nothing (loops back)
				//rf.mu.Lock()
				//fmt.Println(fmt.Sprint(rf.me) + " recieved heratbeat, time: " + fmt.Sprint(time.Now()))
				//rf.mu.Unlock()
			// case 2: timeout, start election
			case <-time.After(time.Duration(float64(time.Second) * (randomTimeout))):
				// could not get heartbeat, start election for itself!

				//fmt.Println("randomtimeout of the following: " + fmt.Sprint(randomTimeout))

				rf.startElection()

			}

		} else if rf.state == 1 {
			rf.mu.Unlock()

			// server is a candidate, garunteeing 3 things:
			// 1: a leader has not sent heartbeats to it
			// 2: it has not granted a vote to another server
			// 3: has not seen a higher term

			// collect all the votes

			// use go routine here for the election process, as other servers might not have replied quick enough
			rf.mu.Lock()

			// for resetting the winning channel, just incase previous run made them win later
			if len(rf.wonElecChan) > 0 {
				fmt.Println("shouldn't be here???======================================================================")
				<-rf.wonElecChan
			}
			rf.mu.Unlock()

			//go rf.collectVotes()

			// now there are 3 cases for a candidate:

			select {
			// case 1: got heartbeat from either voting (for a higher term) or new leader
			case <-rf.heartbeatChan:
				//fmt.Println("NEW CASE: heartbeat as follower")
				rf.mu.Lock()
				rf.state = 0
				rf.mu.Unlock()

			// case 2: win the election
			case <-rf.wonElecChan:
				// change to leader status
				rf.mu.Lock()
				rf.state = 2
				rf.mu.Unlock()

			// case 3: timeout before winning, maybe a tie, leader would have sent heartbeat otherwise
			case <-time.After(time.Duration(float64(time.Second) * (randomTimeout))):

				rf.mu.Lock()
				if rf.state == 1 {
					//fmt.Println(fmt.Sprint(rf.me) + " starting a new election")
					rf.mu.Unlock()
					// if server remains a candidate (no heartbeats or no vote granted or not seen higher term)
					// then re do the election

					rf.startElection()

				} else if rf.state == 0 {
					rf.mu.Unlock()
					// server changed into a follower at some point
				} else {
					rf.mu.Unlock()
					fmt.Println("error: SHOULD NOT BE HERE ==================")
				}

			}
		} else if rf.state == 2 {
			rf.mu.Unlock()

			// server is a leader

			// send heartbeats to other servers every X miliseconds (has to be less than timeout)

			// send heartbeat first, then sleep b/c once elected leader has to immediately send out heartbeats upon election

			for server := 0; server < len(rf.peers); server++ {

				if server != rf.me { // don't send heartbeat to itself

					rf.mu.Lock()
					//fmt.Println("CURRENT TIME: " + fmt.Sprint(time.Now()))
					prevLogIndex := len(rf.log) - 1 // no new log entries, -1
					prevLogTerm := rf.log[prevLogIndex].Term
					entries := []logEntrae{} // EMPTY for heartbeats
					leaderCommit := rf.commitIndex

					fmt.Println("Server " + fmt.Sprint(rf.me) + ", AppendEntries to " + fmt.Sprint(server) + ": " + "rf.currentTerm-> " + fmt.Sprint(rf.currentTerm) + ", " + "rf.me-> " + fmt.Sprint(rf.me) + ", " + "prevLogIndex-> " + fmt.Sprint(prevLogIndex) + ", " + "prevLogTerm-> " + fmt.Sprint(prevLogTerm) + ", " + "entries-> " + fmt.Sprint(entries) + ", " + "rf.leaderCommit-> " + fmt.Sprint(leaderCommit))
					args := AppendEntriesArgs{rf.currentTerm, rf.me, prevLogIndex, prevLogTerm, entries, leaderCommit} // args for append entries RPC
					reply := AppendEntriesReply{}
					rf.mu.Unlock()

					// send out append entries heartbeats instantly for each one
					go rf.sendAppendEntries(server, &args, &reply)
				}
			}

			heartBeatTime := 100 * time.Millisecond // heartbeat every 0.2 second, careful: diff formula now
			//100.0

			time.Sleep(heartBeatTime)

			// use append entries RPC and call it every 0.1 seconds in order to send heartbeats to other servers

		} else {

			// error
			fmt.Println("error: not supposed to be here")
		}

	}
}

// The service or tester wants to create a Raft server. The ports
// of all the Raft servers (including this one) are in peers[]. This
// server's port is peers[me]. All the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	//fmt.Println("CREATED A RAFT INSTANCE FOR: " + fmt.Sprint(me))

	// Your initialization code here (4A, 4B).

	rf.votedFor = -1   // set defualt to -1, meaning did not vote for anyone
	rf.currentTerm = 0 // set defualt term to 0

	rf.heartbeatChan = make(chan int, 1) // create heart beat channel for length 1
	rf.state = 0                         // set default to follower
	//rf.RPCVoteChan = make(chan VoteStatus, len(peers))     // create RPC vote request collector channel
	rf.wonElecChan = make(chan bool, 1) // create election winning channel

	rf.commitIndex = 0            // init highest known commit index to 0
	rf.log = make([]logEntrae, 1) // init dummy for first one to enforce 1-index
	rf.log[0].Term = -1

	rf.applyCh = applyCh // init apply channel for leader to commit entries
	rf.lastApplied = 0   // init to 0

	// initialize from state persisted before a crash.
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections.
	go rf.ticker()

	return rf
}
