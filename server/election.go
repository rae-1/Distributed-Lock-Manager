package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	pb "lock-service/lock"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Constants for server states
const (
	FOLLOWER  = "follower"
	CANDIDATE = "candidate"
	LEADER    = "leader"
)

// Constants for timeouts (milliseconds)
const (
	MIN_ELECTION_TIMEOUT  = 2500
	MAX_ELECTION_TIMEOUT  = 15000
	HEARTBEAT_INTERVAL    = 700
	VOTE_RESPONSE_TIMEOUT = 500
	RPC_TIMEOUT           = 500
)

// Initialize election components of the server
func (s *server) InitializeElection() {
	s.serverState = FOLLOWER
	s.votedFor = -1
	s.votes = make(map[int32]bool)
	s.heartbeatInterval = HEARTBEAT_INTERVAL * time.Millisecond

	// Start with a random election timeout
	s.resetElectionTimer()

	// Start the election monitor
	go s.runElectionMonitor()
}

// RunElectionMonitor continuously monitors and manages the election state
func (s *server) runElectionMonitor() {
	for {
		state := s.getState()
		switch state {
		case FOLLOWER:
			// In follower state, wait for the election timer to expire
			log.Printf("Server %d: Waiting for election timer to expire...", s.serverId)

			// Capture current timer to avoid racing with resetElectionTimer
			s.lockTimerMutex.Lock()
			currentTimer := s.electionTimer
			timerGeneration := s.timerGeneration // Add a generation counter
			s.lockTimerMutex.Unlock()

			// Create a channel to signal timer reset
			timerResetCh := make(chan struct{})

			// Start goroutine to monitor timer generation
			go func() {
				for {
					time.Sleep(500 * time.Millisecond)
					s.lockTimerMutex.Lock()
					if s.timerGeneration != timerGeneration {
						// Timer was reset, signal to exit the wait
						s.lockTimerMutex.Unlock()
						close(timerResetCh)
						return
					}
					s.lockTimerMutex.Unlock()
				}
			}()

			// Wait for either timer expiration or reset
			select {
			case <-currentTimer.C:
				// Timer expired normally
				if s.getState() == FOLLOWER {
					log.Printf("Server %d: Election timer expired, starting election", s.serverId)
					s.startElection()
				}
			case <-timerResetCh:
				// Timer was reset (due to heartbeat), restart the loop
				log.Printf("Server %d: Timer was reset, restarting monitor", s.serverId)
				continue
			}

		case CANDIDATE:
			// In candidate state, wait for either:
			// 1. Election success (becoming leader)
			// 2. Another leader emerging (becoming follower)
			// 3. Election timeout (starting new election)
			log.Printf("Server %d: Waiting for candidate election timeout...", s.serverId)

			// Capture current timer to avoid racing with resetElectionTimer
			s.lockTimerMutex.Lock()
			currentTimer := s.electionTimer
			timerGeneration := s.timerGeneration
			s.lockTimerMutex.Unlock()

			// Create a channel to signal timer reset
			timerResetCh := make(chan struct{})

			// Start goroutine to monitor timer generation
			go func() {
				for {
					time.Sleep(500 * time.Millisecond)
					s.lockTimerMutex.Lock()
					if s.timerGeneration != timerGeneration {
						// Timer was reset, signal to exit the wait
						s.lockTimerMutex.Unlock()
						close(timerResetCh)
						return
					}
					s.lockTimerMutex.Unlock()
				}
			}()

			// Wait for either timer expiration or reset
			select {
			case <-currentTimer.C:
				// Timer expired normally
				if s.getState() == CANDIDATE {
					log.Printf("Server %d: Election timed out, starting new election", s.serverId)
					s.startElection()
				}
			case <-timerResetCh:
				// Timer was reset (due to becoming follower or leader), restart the loop
				log.Printf("Server %d: Timer was reset during candidacy, restarting monitor", s.serverId)
				continue
			}

		case LEADER:
			// In leader state, send heartbeats periodically
			time.Sleep(s.heartbeatInterval)
			s.sendHeartbeats()
		}
	}
}

// Reset the election timer with a random timeout
func (s *server) resetElectionTimer() {
	s.lockTimerMutex.Lock()
	defer s.lockTimerMutex.Unlock()

	if s.electionTimer != nil {
		s.electionTimer.Stop()
	}

	// Random timeout between MIN_ELECTION_TIMEOUT and MAX_ELECTION_TIMEOUT
	timeout := MIN_ELECTION_TIMEOUT + rand.Intn(MAX_ELECTION_TIMEOUT-MIN_ELECTION_TIMEOUT)
	s.electionTimeout = time.Duration(timeout) * time.Millisecond
	s.timerGeneration++ // Increment the generation counter
	s.electionTimer = time.NewTimer(s.electionTimeout)
	log.Printf("Server %d: Reset election timer to %v", s.serverId, s.electionTimeout)
}

// Start a new election
func (s *server) startElection() {
	s.stateMutex.Lock()

	// Only start election if we're not already a leader
	if s.serverState == LEADER {
		s.stateMutex.Unlock()
		return
	}

	// Increment term and update state
	s.termMutex.Lock()
	s.currentTerm++
	currentTerm := s.currentTerm
	s.termMutex.Unlock()

	// Update state to candidate
	s.serverState = CANDIDATE
	s.votedFor = s.serverId // Vote for self

	// Reset votes and count self-vote
	s.votesMutex.Lock()
	s.votes = make(map[int32]bool)
	s.votes[s.serverId] = true
	s.votesMutex.Unlock()

	s.stateMutex.Unlock()

	// Reset election timer for next election if this one fails
	s.resetElectionTimer()

	log.Printf("Server %d: Starting election for term %d", s.serverId, currentTerm)

	// Calculate total sum of processed requests for up-to-date check
	s.processedRequestsMutex.Lock()
	requestSum := int64(0)
	for _, seqNum := range s.processedRequests {
		requestSum += seqNum
	}
	s.processedRequestsMutex.Unlock()

	// Send RequestVote RPCs to all other servers in parallel
	var wg sync.WaitGroup
	votesReceived := 1 // Count self-vote
	var votesMutex sync.Mutex

	for i, serverAddr := range s.serverList {
		if int32(i) == s.serverId {
			continue // Skip self
		}

		wg.Add(1)
		go func(serverIndex int, serverAddr string) {
			defer wg.Done()

			// Prepare vote request
			voteRequest := &pb.VoteRequest{
				Term:                currentTerm,
				CandidateId:         s.serverId,
				LastLogIndex:        s.currentLogNumber,
				LastLogTerm:         currentTerm,
				ProcessedRequestSum: requestSum,
			}

			// Check if we're still a candidate before sending request
			if s.getState() != CANDIDATE {
				return
			}

			log.Printf("Server %d: Requesting vote from server %d for term %d",
				s.serverId, serverIndex, currentTerm)

			// Connect to the server
			ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT*time.Millisecond)
			defer cancel()

			// Send vote request
			response, err := s.requestVoteRPC(ctx, serverAddr, voteRequest)
			if err != nil {
				log.Printf("sending beats3")
				log.Printf("Server %d: Failed to get vote from server %d: %v",
					s.serverId, serverIndex, err)
				return
			}

			// Check if we're still in the same term (could have changed since request was sent)
			s.termMutex.Lock()
			if response.Term > s.currentTerm {
				// Discovered higher term, revert to follower
				log.Printf("Server %d: Discovered higher term %d from server %d",
					s.serverId, response.Term, serverIndex)
				s.becomeFollower(response.Term, -1)
				s.termMutex.Unlock()
				return
			}
			s.termMutex.Unlock()

			// If vote granted, update count
			if response.VoteGranted {
				log.Printf("Server %d: Received vote from server %d", s.serverId, serverIndex)
				votesMutex.Lock()
				votesReceived++
				votesMutex.Unlock()

				// Check if we have majority
				votesMutex.Lock()
				majority := (len(s.serverList) / 2) + 1
				if votesReceived >= majority && s.getState() == CANDIDATE {
					log.Printf("Server %d: Won election for term %d with %d votes",
						s.serverId, currentTerm, votesReceived)
					votesMutex.Unlock()
					s.becomeLeader()
					return
				}
				votesMutex.Unlock()
			} else {
				log.Printf("Server %d: Vote denied by server %d", s.serverId, serverIndex)
			}
		}(i, serverAddr)
	}

	// Wait for all vote requests to complete (or timeout)
	waitCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitCh)
	}()

	select {
	case <-waitCh:
		// All votes received or failed
	case <-time.After(VOTE_RESPONSE_TIMEOUT * time.Millisecond):
		// Timeout waiting for votes
		log.Printf("Server %d: Timeout waiting for vote responses", s.serverId)
	}

	// Check if we won the election
	votesMutex.Lock()
	majority := (len(s.serverList) / 2) + 1
	if votesReceived >= majority && s.getState() == CANDIDATE {
		log.Printf("Server %d: Won election for term %d with %d votes",
			s.serverId, currentTerm, votesReceived)
		votesMutex.Unlock()
		s.becomeLeader()
	} else {
		votesMutex.Unlock()
	}
}

// RequestVote RPC handler - implements pb.LockServiceServer
func (s *server) RequestVote(ctx context.Context, req *pb.VoteRequest) (*pb.VoteResponse, error) {
	s.termMutex.Lock()
	defer s.termMutex.Unlock()

	log.Printf("Server %d: Received vote request from %d for term %d (current term: %d)",
		s.serverId, req.CandidateId, req.Term, s.currentTerm)

	// Prepare default response
	response := &pb.VoteResponse{
		Term:        s.currentTerm,
		VoteGranted: false,
	}

	// Rule 1: If term < currentTerm, reject vote
	if req.Term < s.currentTerm {
		log.Printf("Server %d: Rejecting vote for %d - lower term", s.serverId, req.CandidateId)
		return response, nil
	}

	// If term > currentTerm, update term and become follower
	if req.Term > s.currentTerm {
		log.Printf("Server %d: Discovered higher term %d, updating", s.serverId, req.Term)
		s.currentTerm = req.Term
		s.becomeFollower(req.Term, -1)
		s.votedFor = -1
	}

	// Check if we've already voted or can vote for this candidate
	voteGranted := false

	// If we haven't voted for anyone yet in this term, or we've already voted for this candidate
	if s.votedFor == -1 || s.votedFor == req.CandidateId {
		// Check if candidate is at least as up-to-date as us (using your criteria)
		// 1. First check term
		if req.LastLogTerm > s.currentTerm {
			voteGranted = true
		} else if req.LastLogTerm == s.currentTerm {
			// 2. If terms equal, check processed request sum
			s.processedRequestsMutex.Lock()
			localSum := int64(0)
			for _, seqNum := range s.processedRequests {
				localSum += seqNum
			}
			s.processedRequestsMutex.Unlock()

			if req.ProcessedRequestSum > localSum {
				voteGranted = true
			} else if req.ProcessedRequestSum == localSum {
				// 3. If sums equal, check log number
				if req.LastLogIndex >= s.currentLogNumber {
					voteGranted = true
				}
			}
		}

		if voteGranted {
			// Grant vote
			s.votedFor = req.CandidateId
			response.VoteGranted = true
			log.Printf("Server %d: Granted vote to %d for term %d",
				s.serverId, req.CandidateId, req.Term)

			// Reset election timer since we granted a vote
			s.resetElectionTimer()
		} else {
			log.Printf("Server %d: Rejected vote for %d - not up-to-date",
				s.serverId, req.CandidateId)
		}
	} else {
		log.Printf("Server %d: Rejected vote for %d - already voted for %d",
			s.serverId, req.CandidateId, s.votedFor)
	}

	response.Term = s.currentTerm
	response.VoteGranted = voteGranted
	return response, nil
}

// Heartbeat RPC handler - implements pb.LockServiceServer
func (s *server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	s.termMutex.Lock()
	defer s.termMutex.Unlock()

	log.Printf("Server %d: Received heartbeat from leader %d (term %d, current term: %d)",
		s.serverId, req.LeaderId, req.Term, s.currentTerm)

	response := &pb.HeartbeatResponse{
		Term:    s.currentTerm,
		Success: false,
	}

	// If heartbeat term is smaller than our term, reject it
	if req.Term < s.currentTerm {
		log.Printf("Server %d: Rejecting heartbeat from %d - lower term",
			s.serverId, req.LeaderId)
		return response, nil
	}

	// If heartbeat term is greater than or equal, accept leader and update
	if req.Term >= s.currentTerm {
		// Update term if necessary
		if req.Term > s.currentTerm {
			s.currentTerm = req.Term
		}

		// Become follower and acknowledge the leader
		s.becomeFollower(req.Term, req.LeaderId)

		// Reset election timer since we received valid heartbeat
		s.resetElectionTimer()

		// Update processed requests from leader
		if req.ProcessedRequests != nil {
			s.processedRequestsMutex.Lock()
			for clientID, seqNum := range req.ProcessedRequests {
				currentSeq, exists := s.processedRequests[clientID]
				if !exists || seqNum > currentSeq {
					s.processedRequests[clientID] = seqNum
				}
			}
			s.processedRequestsMutex.Unlock()
		}

		response.Success = true
		log.Printf("Server %d: Accepted heartbeat from leader %d", s.serverId, req.LeaderId)
	}

	return response, nil
}

// Send heartbeats to all other servers
func (s *server) sendHeartbeats() {
	// Only leaders should send heartbeats
	if s.getState() != LEADER {
		return
	}

	s.termMutex.Lock()
	currentTerm := s.currentTerm
	s.termMutex.Unlock()

	// Prepare heartbeat request
	s.processedRequestsMutex.Lock()
	processedRequestsCopy := make(map[int32]int64)
	for clientID, seqNum := range s.processedRequests {
		processedRequestsCopy[clientID] = seqNum
	}
	s.processedRequestsMutex.Unlock()

	heartbeatRequest := &pb.HeartbeatRequest{
		Term:              currentTerm,
		LeaderId:          s.serverId,
		ProcessedRequests: processedRequestsCopy,
	}

	// Send heartbeats to all other servers in parallel
	var wg sync.WaitGroup

	for i, serverAddr := range s.serverList {
		if int32(i) == s.serverId {
			continue // Skip self
		}

		wg.Add(1)
		go func(serverIndex int, serverAddr string) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT*time.Millisecond)
			defer cancel()

			// Send heartbeat
			response, err := s.heartbeatRPC(ctx, serverAddr, heartbeatRequest)
			if err != nil {
				log.Printf("Server %d: Failed to send heartbeat to server %d: %v",
					s.serverId, serverIndex, err)
				return
			}

			// If receiver has higher term, step down
			if response.Term > currentTerm {
				log.Printf("Server %d: Discovered higher term %d from server %d during heartbeat",
					s.serverId, response.Term, serverIndex)

				s.termMutex.Lock()
				if response.Term > s.currentTerm {
					s.becomeFollower(response.Term, -1)
				}
				s.termMutex.Unlock()
			}
		}(i, serverAddr)
	}

	// Wait for all heartbeats to complete
	wg.Wait()
}

// Become leader
func (s *server) becomeLeader() {
	s.stateMutex.Lock()
	defer s.stateMutex.Unlock()

	if s.serverState == LEADER {
		return // Already a leader
	}

	log.Printf("Server %d: Becoming leader for term %d", s.serverId, s.currentTerm)

	s.serverState = LEADER
	s.leaderIndex = int(s.serverId)

	// Send immediate heartbeat to establish authority
	go s.sendHeartbeats()
}

// Become follower
func (s *server) becomeFollower(term int64, leaderId int32) {
	s.stateMutex.Lock()
	defer s.stateMutex.Unlock()

	oldState := s.serverState
	s.serverState = FOLLOWER

	// Update term and votedFor
	// s.termMutex.Lock()
	if term > s.currentTerm {
		s.currentTerm = term
		s.votedFor = -1 // Reset vote for new term
	}
	// s.termMutex.Unlock()

	// Update leader if valid
	if leaderId >= 0 {
		s.leaderIndex = int(leaderId)
	}

	// Reset election timer
	if oldState != FOLLOWER {
		s.resetElectionTimer()
		log.Printf("Server %d: Became follower in term %d (leader: %d)",
			s.serverId, term, leaderId)
	}
}

// Get current server state safely
func (s *server) getState() string {
	s.stateMutex.Lock()
	defer s.stateMutex.Unlock()
	return s.serverState
}

// GetLeader RPC implementation
func (s *server) GetLeader(ctx context.Context, in *pb.Int) (*pb.LeaderInfo, error) {
	return &pb.LeaderInfo{
		LeaderId:      int32(s.leaderIndex),
		LeaderAddress: s.serverList[s.leaderIndex],
	}, nil
}

// Helper function to send RequestVote RPC
func (s *server) requestVoteRPC(ctx context.Context, serverAddr string, req *pb.VoteRequest) (*pb.VoteResponse, error) {
	// Establish connection
	conn, err := s.getServerConnection(serverAddr)
	if err != nil {
		log.Printf("sending beats2")
		return nil, err
	}

	client := pb.NewLockServiceClient(conn)
	return client.RequestVote(ctx, req)
}

// Helper function to send Heartbeat RPC
func (s *server) heartbeatRPC(ctx context.Context, serverAddr string, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	// Establish connection
	conn, err := s.getServerConnection(serverAddr)
	if err != nil {
		return nil, err
	}

	client := pb.NewLockServiceClient(conn)
	return client.Heartbeat(ctx, req)
}

// Helper to get a gRPC connection to another server
// In a real implementation, you'd want to cache and reuse connections
func (s *server) getServerConnection(serverAddr string) (*grpc.ClientConn, error) {
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("sending beats")
		return nil, fmt.Errorf("failed to connect to server %s: %v", serverAddr, err)
	}
	return conn, nil
}
