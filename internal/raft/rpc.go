package raft

// -------------------------------------------------------
// RPC MESSAGE TYPES
// Raft uses two core RPCs:
//   1. RequestVote   — candidates ask peers for votes during election
//   2. AppendEntries — leader sends log entries (or heartbeats) to followers
// -------------------------------------------------------

// RequestVoteArgs is sent by a Candidate to all peers asking for their vote
type RequestVoteArgs struct {
	Term         int    // Candidate's current term
	CandidateID  string // Who is asking for the vote
	LastLogIndex int    // Index of candidate's last log entry
	LastLogTerm  int    // Term of candidate's last log entry
}

// RequestVoteReply is the response from a peer
type RequestVoteReply struct {
	Term        int  // Peer's current term (so candidate can update itself)
	VoteGranted bool // true = peer voted for this candidate
}

// AppendEntriesArgs is sent by the Leader to replicate log entries
// When Entries is empty, it acts as a heartbeat (no new data, just "I'm alive")
type AppendEntriesArgs struct {
	Term         int        // Leader's current term
	LeaderID     string     // So followers can redirect clients
	PrevLogIndex int        // Index of log entry before the new ones
	PrevLogTerm  int        // Term of PrevLogIndex entry
	Entries      []LogEntry // New entries to store (empty for heartbeat)
	LeaderCommit int        // Leader's commitIndex
}

// AppendEntriesReply is the follower's response
type AppendEntriesReply struct {
	Term    int  // Follower's current term (so leader can step down if stale)
	Success bool // true if follower matched PrevLogIndex/PrevLogTerm
}

// -------------------------------------------------------
// RPC HANDLERS (called on the receiving node)
// -------------------------------------------------------

// HandleRequestVote processes an incoming vote request.
// A node grants its vote if:
//   1. The candidate's term is >= our term
//   2. We haven't voted for anyone else this term
//   3. The candidate's log is at least as up-to-date as ours
func (n *Node) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := RequestVoteReply{Term: n.CurrentTerm}

	// Rule 1: If we see a higher term, update ours and revert to Follower
	if args.Term > n.CurrentTerm {
		n.CurrentTerm = args.Term
		n.Role = Follower
		n.VotedFor = ""
	}

	// Deny vote if candidate's term is stale
	if args.Term < n.CurrentTerm {
		reply.VoteGranted = false
		return reply
	}

	// Deny vote if we already voted for someone else this term
	alreadyVoted := n.VotedFor != "" && n.VotedFor != args.CandidateID
	if alreadyVoted {
		reply.VoteGranted = false
		return reply
	}

	// Deny vote if our log is more up-to-date than candidate's
	// (Log completeness check — ensures only nodes with full data can lead)
	ourLastTerm := n.lastLogTerm()
	ourLastIndex := n.lastLogIndex()
	logIsStale := args.LastLogTerm < ourLastTerm ||
		(args.LastLogTerm == ourLastTerm && args.LastLogIndex < ourLastIndex)
	if logIsStale {
		reply.VoteGranted = false
		return reply
	}

	// Grant the vote!
	n.VotedFor = args.CandidateID
	reply.VoteGranted = true
	return reply
}

// HandleAppendEntries processes an incoming AppendEntries RPC from the leader.
// This is the heart of log replication — and doubles as a heartbeat.
func (n *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := AppendEntriesReply{Term: n.CurrentTerm}

	// Reject if leader's term is stale
	if args.Term < n.CurrentTerm {
		reply.Success = false
		return reply
	}

	// Valid leader — reset our election timer by signaling HeartbeatCh
	n.Role = Follower
	n.CurrentTerm = args.Term
	select {
	case n.HeartbeatCh <- struct{}{}:
	default:
	}

	// Consistency check: verify our log matches at PrevLogIndex/PrevLogTerm
	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex > len(n.Log) {
			// We're missing entries — reject so leader backs up NextIndex
			reply.Success = false
			return reply
		}
		if n.Log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			// Conflicting entry — reject and let leader retry with earlier index
			reply.Success = false
			return reply
		}
	}

	// Append new entries (overwriting any conflicting ones)
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + i
		if idx < len(n.Log) {
			n.Log[idx] = entry // Overwrite conflict
		} else {
			n.Log = append(n.Log, entry) // Append new
		}
	}

	// Advance commit index if leader says more entries are committed
	if args.LeaderCommit > n.CommitIndex {
		newCommit := args.LeaderCommit
		if lastIdx := n.lastLogIndex(); lastIdx < newCommit {
			newCommit = lastIdx
		}
		// Apply all newly committed entries
		for n.LastApplied < newCommit {
			n.LastApplied++
			n.ApplyCh <- n.Log[n.LastApplied-1]
		}
		n.CommitIndex = newCommit
	}

	reply.Success = true
	return reply
}
