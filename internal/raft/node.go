package raft

import (
	"sync"
	"time"
)

// Role represents the current state of a Raft node
type Role int

const (
	Follower  Role = iota // Default state — listens to leader
	Candidate             // Trying to become leader
	Leader                // Handles all client writes
)

// LogEntry is a single command in the replicated log
// Every deposit/withdraw/transfer becomes a LogEntry
type LogEntry struct {
	Term    int    // Which election term this was created in
	Index   int    // Position in the log
	Command string // e.g. "DEPOSIT:acc123:500"
}

// Node is the core Raft state machine
type Node struct {
	mu sync.Mutex

	// Identity
	ID      string   // This node's ID (e.g. "node1")
	Peers   []string // Addresses of other nodes (e.g. ["node2:8081", "node3:8082"])

	// Persistent Raft state (must survive crashes)
	CurrentTerm int        // Latest term this node has seen
	VotedFor    string     // CandidateID this node voted for in current term ("" if none)
	Log         []LogEntry // The replicated command log

	// Volatile state
	CommitIndex int  // Highest log entry known to be committed
	LastApplied int  // Highest log entry applied to state machine
	Role        Role // Current role: Follower, Candidate, or Leader

	// Leader-only state (reinitialized after each election)
	NextIndex  map[string]int // For each peer: next log index to send
	MatchIndex map[string]int // For each peer: highest log index known to be replicated

	// Channels for internal signaling
	HeartbeatCh chan struct{} // Reset election timer when heartbeat received
	ApplyCh     chan LogEntry // Committed entries are sent here to be applied

	// Election timeout (randomized to avoid split votes)
	ElectionTimeout time.Duration
}

// NewNode creates a new Raft node in Follower state
func NewNode(id string, peers []string, applyCh chan LogEntry) *Node {
	return &Node{
		ID:              id,
		Peers:           peers,
		Role:            Follower,
		CurrentTerm:     0,
		VotedFor:        "",
		Log:             []LogEntry{},
		CommitIndex:     0,
		LastApplied:     0,
		NextIndex:       make(map[string]int),
		MatchIndex:      make(map[string]int),
		HeartbeatCh:     make(chan struct{}, 1),
		ApplyCh:         applyCh,
		ElectionTimeout: randomTimeout(150, 300),
	}
}

// lastLogIndex returns the index of the last log entry (0 if empty)
func (n *Node) lastLogIndex() int {
	if len(n.Log) == 0 {
		return 0
	}
	return n.Log[len(n.Log)-1].Index
}

// lastLogTerm returns the term of the last log entry (0 if empty)
func (n *Node) lastLogTerm() int {
	if len(n.Log) == 0 {
		return 0
	}
	return n.Log[len(n.Log)-1].Term
}

// IsLeader safely checks if this node is the current leader
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.Role == Leader
}

// randomTimeout returns a random duration between min and max milliseconds.
// Raft requires randomized timeouts so nodes don't all call elections simultaneously.
func randomTimeout(minMs, maxMs int) time.Duration {
	// Using a simple spread — in production you'd use crypto/rand
	spread := maxMs - minMs
	ms := minMs + (int(time.Now().UnixNano()) % spread)
	if ms < minMs {
		ms = minMs
	}
	return time.Duration(ms) * time.Millisecond
}
