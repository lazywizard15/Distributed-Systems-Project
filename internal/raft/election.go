package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Run starts the Raft node's main event loop.
// It runs forever, switching between:
//   - Follower: waiting for heartbeats from a leader
//   - Candidate: running an election
//   - Leader: sending heartbeats and replicating log entries
func (n *Node) Run() {
	for {
		n.mu.Lock()
		role := n.Role
		n.mu.Unlock()

		switch role {
		case Follower:
			n.runFollower()
		case Candidate:
			n.runCandidate()
		case Leader:
			n.runLeader()
		}
	}
}

// runFollower waits for a heartbeat. If the election timeout fires first,
// it means the leader is dead — transition to Candidate.
func (n *Node) runFollower() {
	timeout := randomTimeout(150, 300)
	select {
	case <-n.HeartbeatCh:
		// Received heartbeat — stay Follower, reset timer (select re-runs)
	case <-time.After(timeout):
		// Timeout — no leader heard from. Start an election!
		n.mu.Lock()
		n.Role = Candidate
		n.mu.Unlock()
	}
}

// runCandidate starts a new election:
//  1. Increments term
//  2. Votes for itself
//  3. Asks all peers for votes
//  4. If majority received → become Leader
//  5. If another leader appears → revert to Follower
//  6. If timeout → start another election
func (n *Node) runCandidate() {
	n.mu.Lock()
	n.CurrentTerm++
	n.VotedFor = n.ID
	term := n.CurrentTerm
	lastIndex := n.lastLogIndex()
	lastTerm := n.lastLogTerm()
	peers := n.Peers
	n.mu.Unlock()

	fmt.Printf("[%s] Starting election for term %d\n", n.ID, term)

	votes := 1 // Vote for ourselves
	voteCh := make(chan bool, len(peers))

	// Ask all peers for votes concurrently
	for _, peer := range peers {
		go func(peer string) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateID:  n.ID,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			}
			reply, err := sendRequestVote(peer, args)
			if err != nil {
				voteCh <- false
				return
			}
			// If peer has higher term, we're stale — step down
			if reply.Term > term {
				n.mu.Lock()
				n.CurrentTerm = reply.Term
				n.Role = Follower
				n.VotedFor = ""
				n.mu.Unlock()
			}
			voteCh <- reply.VoteGranted
		}(peer)
	}

	// Count votes with a timeout
	timeout := randomTimeout(150, 300)
	majority := (len(peers)+1)/2 + 1

	for i := 0; i < len(peers); i++ {
		select {
		case granted := <-voteCh:
			if granted {
				votes++
			}
			if votes >= majority {
				// Won the election!
				n.becomeLeader()
				return
			}
		case <-time.After(timeout):
			// Split vote or timeout — try again next round
			return
		case <-n.HeartbeatCh:
			// Someone else became leader — step down
			n.mu.Lock()
			n.Role = Follower
			n.mu.Unlock()
			return
		}
	}
}

// becomeLeader initializes leader state and transitions to Leader role
func (n *Node) becomeLeader() {
	n.mu.Lock()
	defer n.mu.Unlock()

	fmt.Printf("[%s] Became leader for term %d\n", n.ID, n.CurrentTerm)
	n.Role = Leader

	// Initialize NextIndex for each peer to leader's last log index + 1
	for _, peer := range n.Peers {
		n.NextIndex[peer] = n.lastLogIndex() + 1
		n.MatchIndex[peer] = 0
	}
}

// runLeader sends periodic heartbeats to all followers.
// Heartbeats also carry any new log entries (AppendEntries doubles as both).
func (n *Node) runLeader() {
	// Send heartbeat immediately, then every 50ms
	n.sendHeartbeats()
	time.Sleep(50 * time.Millisecond)

	// Check we're still leader (a higher-term message could have demoted us)
	n.mu.Lock()
	stillLeader := n.Role == Leader
	n.mu.Unlock()

	if !stillLeader {
		return
	}
}

// sendHeartbeats sends AppendEntries to all peers (empty = heartbeat, with entries = replication)
func (n *Node) sendHeartbeats() {
	n.mu.Lock()
	peers := n.Peers
	term := n.CurrentTerm
	leaderID := n.ID
	commitIndex := n.CommitIndex
	n.mu.Unlock()

	for _, peer := range peers {
		go func(peer string) {
			n.mu.Lock()
			nextIdx := n.NextIndex[peer]
			prevLogIndex := nextIdx - 1
			prevLogTerm := 0
			if prevLogIndex > 0 && prevLogIndex <= len(n.Log) {
				prevLogTerm = n.Log[prevLogIndex-1].Term
			}
			// Send any entries the peer is missing
			entries := []LogEntry{}
			if nextIdx <= len(n.Log) {
				entries = n.Log[nextIdx-1:]
			}
			n.mu.Unlock()

			args := AppendEntriesArgs{
				Term:         term,
				LeaderID:     leaderID,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: commitIndex,
			}

			reply, err := sendAppendEntries(peer, args)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if reply.Term > n.CurrentTerm {
				// We're stale — step down
				n.CurrentTerm = reply.Term
				n.Role = Follower
				n.VotedFor = ""
				return
			}

			if reply.Success {
				// Update tracking for this peer
				n.MatchIndex[peer] = prevLogIndex + len(entries)
				n.NextIndex[peer] = n.MatchIndex[peer] + 1

				// Advance commit index if a majority have replicated this entry
				n.maybeAdvanceCommitIndex()
			} else {
				// Peer rejected — back up and retry
				if n.NextIndex[peer] > 1 {
					n.NextIndex[peer]--
				}
			}
		}(peer)
	}
}

// maybeAdvanceCommitIndex checks if a new entry can be committed.
// An entry is committed once a majority of nodes have it in their log.
// Must be called with n.mu held.
func (n *Node) maybeAdvanceCommitIndex() {
	majority := (len(n.Peers)+1)/2 + 1

	for idx := len(n.Log); idx > n.CommitIndex; idx-- {
		if n.Log[idx-1].Term != n.CurrentTerm {
			continue // Only commit entries from current term (Raft safety rule)
		}
		replicatedCount := 1 // Leader always has it
		for _, peer := range n.Peers {
			if n.MatchIndex[peer] >= idx {
				replicatedCount++
			}
		}
		if replicatedCount >= majority {
			// Commit everything up to this index
			for n.LastApplied < idx {
				n.LastApplied++
				n.ApplyCh <- n.Log[n.LastApplied-1]
			}
			n.CommitIndex = idx
			break
		}
	}
}

// Propose adds a new command to the leader's log and begins replication.
// Returns an error if this node is not the leader.
func (n *Node) Propose(command string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.Role != Leader {
		return fmt.Errorf("not the leader")
	}

	entry := LogEntry{
		Term:    n.CurrentTerm,
		Index:   len(n.Log) + 1,
		Command: command,
	}
	n.Log = append(n.Log, entry)
	fmt.Printf("[%s] Proposed: %s\n", n.ID, command)
	return nil
}

// -------------------------------------------------------
// HTTP helpers to call RPCs on remote nodes
// -------------------------------------------------------

func sendRequestVote(peer string, args RequestVoteArgs) (RequestVoteReply, error) {
	var reply RequestVoteReply
	body, _ := json.Marshal(args)
	resp, err := http.Post("http://"+peer+"/raft/vote", "application/json", bytes.NewReader(body))
	if err != nil {
		return reply, err
	}
	defer resp.Body.Close()
	json.NewDecoder(resp.Body).Decode(&reply)
	return reply, nil
}

func sendAppendEntries(peer string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	var reply AppendEntriesReply
	body, _ := json.Marshal(args)
	resp, err := http.Post("http://"+peer+"/raft/append", "application/json", bytes.NewReader(body))
	if err != nil {
		return reply, err
	}
	defer resp.Body.Close()
	json.NewDecoder(resp.Body).Decode(&reply)
	return reply, nil
}
