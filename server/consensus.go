package main

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "lock-service/lock"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ReplicateAndGetConsensus attempts to get consensus from a majority of servers
// Returns: success (bool), positive servers ([]int), error
func (s *server) ReplicateAndGetConsensus(operation string, clientID int32, seqNum int64) (bool, []int, error) {
	if s.leaderIndex != int(s.serverId) {
		return false, nil, fmt.Errorf("only leader can replicate operations")
	}

	// Create log entry for replication
	s.termMutex.Lock()
	s.currentLogNumber++
	entry := pb.LogEntry{
		Term:      s.currentTerm,
		LogNumber: s.currentLogNumber,
		Operation: operation,
		ClientId:  clientID,
		SeqNum:    seqNum,
		LeaderId:  int32(s.serverId),
	}
	s.termMutex.Unlock()

	// Track success count for majority
	successCount := 1 // Count leader as success
	totalServers := len(s.serverList)
	majority := (totalServers / 2) + 1
	// positiveServers := []int{int(s.serverId)} // Leader always approves its own operation
	positiveServers := []int{}

	// Channel for responses
	type replicationResult struct {
		serverIndex int
		success     bool
		term        int64
		leaderInfo  *pb.LeaderInfo
	}
	resultCh := make(chan replicationResult, totalServers-1)

	// Send to all followers with short timeout
	for i, serverAddr := range s.serverList {
		if i == int(s.serverId) {
			continue // Skip self
		}

		go func(addr string, idx int) {
			// Connect to follower
			conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("Failed to connect to server %s: %v", addr, err)
				resultCh <- replicationResult{idx, false, 0, nil}
				return
			}
			defer conn.Close()

			client := pb.NewLockServiceClient(conn)

			// Send log entry with short timeout
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			resp, err := client.ReplicateLog(ctx, &entry)

			if err != nil {
				log.Printf("Failed to replicate to server %s: %v", addr, err)
				resultCh <- replicationResult{idx, false, 0, nil}
				return
			}

			// Check if server accepted the entry
			if resp.Success {
				log.Printf("Server %s accepted operation %s for client %d", addr, operation, clientID)
				resultCh <- replicationResult{idx, true, resp.Term, resp.LeaderInfo}
			} else {
				// Server rejected entry (maybe higher term)
				log.Printf("Server %s rejected operation %s (term %d)", addr, operation, resp.Term)
				resultCh <- replicationResult{idx, false, resp.Term, resp.LeaderInfo}
			}
		}(serverAddr, i)
	}

	// Wait for all responses or timeout
	for i := 0; i < totalServers-1; i++ {
		result := <-resultCh

		// If we see a higher term, we need to step down as leader
		if result.term > s.currentTerm {
			s.termMutex.Lock()
			s.currentTerm = result.term
			// Update leader information if available
			if result.leaderInfo != nil && result.leaderInfo.LeaderId >= 0 {
				s.leaderIndex = int(result.leaderInfo.LeaderId)
				log.Printf("Updated leader to server %d based on term %d information",
					s.leaderIndex, result.term)
			} else {
				s.leaderIndex = -1 // Unknown leader
			}
			s.termMutex.Unlock()

			// No need to continue checking responses
			return false, positiveServers, fmt.Errorf("discovered higher term %d, stepping down", result.term)
		}

		if result.success {
			successCount++
			positiveServers = append(positiveServers, result.serverIndex)
		}
	}
	close(resultCh)

	// Check if we have majority
	if successCount >= majority {
		log.Printf("Operation %s for client %d committed with majority (%d/%d)",
			operation, clientID, successCount, totalServers)
		return true, positiveServers, nil
	}

	// If we get here, we failed to achieve majority
	log.Printf("Failed to achieve majority for operation %s client %d (%d/%d)",
		operation, clientID, successCount, totalServers)

	return false, positiveServers, fmt.Errorf("failed to replicate to majority")
}

// RollbackOperation sends rollback requests to specified servers
func (s *server) RollbackOperation(term int64, operation string, clientID int32, positiveServers []int) {
	for _, idx := range positiveServers {
		/*
			if idx == int(s.serverId) {
				// Handle local rollback
				s.handleLocalRollback(operation, clientID)
				continue
			}
		*/

		serverAddr := s.serverList[idx]
		go func(addr string) {
			// Connect to server
			conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("Failed to connect to server %s for rollback: %v", addr, err)
				return
			}
			defer conn.Close()

			client := pb.NewLockServiceClient(conn)

			// Send rollback request
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			rollbackReq := &pb.RollbackRequest{
				Term:      term,
				Operation: operation,
				ClientId:  clientID,
			}

			_, err = client.Rollback(ctx, rollbackReq)
			if err != nil {
				log.Printf("Failed to rollback on server %s: %v", addr, err)
			}
		}(serverAddr)
	}
}

// Handle local rollback effects
func (s *server) handleLocalRollback(operation string, clientID int32) {
	switch operation {
	case "client_init":
		// Decrement client counter
		s.clientMutex.Lock()
		if s.clientCounter > 0 {
			s.clientCounter--
		}
		s.clientMutex.Unlock()
		log.Printf("Rolled back client_init for client %d", clientID)

	case "lock_acquire":
		// If this client has the lock, release it
		s.queueMutex.Lock()
		if s.lockHolder == clientID {
			s.lockHolder = 0
			log.Printf("Rolled back lock_acquire for client %d", clientID)
		}
		s.queueMutex.Unlock()

	case "lock_release":
		// Make client the lock holder again if lock is free
		s.queueMutex.Lock()
		if s.lockHolder > 0 {
			// Lock has been given to someone else, put it back at front of queue
			s.waitQueue = append([]int32{s.lockHolder}, s.waitQueue...)
			log.Printf("%d placed at front of wait queue", s.lockHolder)
		}
		s.lockHolder = clientID
		s.lockTimerMutex.Lock()
		s.lockAcquireTime = time.Now()
		s.lockTimerMutex.Unlock()
		log.Printf("Rolled back lock_release for client %d", clientID)
		s.queueMutex.Unlock()

	case "file_append":
		// Decrement sequence number
		s.processedRequestsMutex.Lock()
		if curSeq, exists := s.processedRequests[clientID]; exists && curSeq > 0 {
			s.processedRequests[clientID]--
			log.Printf("Rolled back file_append for client %d (seq_num decremented)", clientID)
		}
		s.processedRequestsMutex.Unlock()

	case "client_close":
		// No action needed for rollback
		log.Printf("No rollback action needed for client_close")
	}
}

// RPC handler for replicate_log
func (s *server) ReplicateLog(ctx context.Context, in *pb.LogEntry) (*pb.LogResponse, error) {
	s.termMutex.Lock()

	log.Printf("Received log replication: term=%d, log=%d, op=%s, client=%d",
		in.Term, in.LogNumber, in.Operation, in.ClientId)

	// Reject if we have a higher term
	if in.Term < s.currentTerm {
		log.Printf("Rejecting operation: sender term %d < our term %d", in.Term, s.currentTerm)
		s.termMutex.Unlock()
		return &pb.LogResponse{
			Term:    s.currentTerm,
			Success: false,
			LeaderInfo: &pb.LeaderInfo{
				LeaderId:      int32(s.leaderIndex),
				LeaderAddress: s.serverList[s.leaderIndex],
			},
		}, nil
	}

	// Update term if necessary
	if in.Term > s.currentTerm {
		s.currentTerm = in.Term
		s.leaderIndex = int(in.LeaderId)
		log.Printf("Updated leader to server %d based on term %d", s.leaderIndex, in.Term)
	}

	// Update log number
	s.currentLogNumber = in.LogNumber
	s.termMutex.Unlock()

	// Apply the operation
	s.applyOperation(in.Operation, in.ClientId, in.SeqNum)

	log.Printf("Accepted operation %s for client %d", in.Operation, in.ClientId)
	return &pb.LogResponse{
		Term:    s.currentTerm,
		Success: true,
		LeaderInfo: &pb.LeaderInfo{
			LeaderId: int32(s.leaderIndex),
			LeaderAddress: func() string {
				if s.leaderIndex >= 0 {
					return s.serverList[s.leaderIndex]
				}
				return ""
			}(),
		},
	}, nil
}

// Apply an operation to the state machine
func (s *server) applyOperation(operation string, clientID int32, seqNum int64) {
	switch operation {
	case "client_init":
		// For followers, just update client counter
		s.clientMutex.Lock()
		if int32(clientID) > s.clientCounter {
			s.clientCounter = clientID
		}
		s.clientMutex.Unlock()
		log.Printf("Applied client_init for client %d", clientID)

	case "lock_acquire":
		s.queueMutex.Lock()
		// if s.lockHolder == 0 {
		s.lockHolder = clientID
		s.lockTimerMutex.Lock()
		s.lockAcquireTime = time.Now()
		s.lockTimerMutex.Unlock()
		log.Printf("Applied lock_acquire for client %d", clientID)
		// } else {
		// 	s.waitQueue = append(s.waitQueue, clientID)
		// 	log.Printf("Added client %d to wait queue", clientID)
		// }
		s.queueMutex.Unlock()

	case "lock_release":
		s.queueMutex.Lock()
		if s.lockHolder == clientID {
			if len(s.waitQueue) > 0 {
				s.lockHolder = s.waitQueue[0]
				s.waitQueue = s.waitQueue[1:]
				s.lockTimerMutex.Lock()
				s.lockAcquireTime = time.Now()
				s.lockTimerMutex.Unlock()
				log.Printf("Lock transferred to client %d", s.lockHolder)
			} else {
				s.lockHolder = 0
				log.Printf("Lock released and now free")
			}
		}
		s.queueMutex.Unlock()

	case "file_append":
		// For all servers, update the processed request counter
		s.processedRequestsMutex.Lock()
		s.processedRequests[clientID] = seqNum
		s.processedRequestsMutex.Unlock()
		log.Printf("Updated sequence number for client %d to %d", clientID, seqNum)

	case "client_close":
		s.queueMutex.Lock()
		if s.lockHolder == clientID {
			if len(s.waitQueue) > 0 {
				s.lockHolder = s.waitQueue[0]
				s.waitQueue = s.waitQueue[1:]
				log.Printf("Lock transferred to client %d after previous holder disconnected", s.lockHolder)
			} else {
				s.lockHolder = 0
				log.Printf("Lock released after holder disconnected")
			}
		}
		s.lockHolder = 0

		// Remove client from wait queue if present
		for i, id := range s.waitQueue {
			if id == clientID {
				s.waitQueue = append(s.waitQueue[:i], s.waitQueue[i+1:]...)
				break
			}
		}
		s.queueMutex.Unlock()
		log.Printf("Applied client_close for client %d", clientID)
	}
}

// RPC handler for rollback
func (s *server) Rollback(ctx context.Context, in *pb.RollbackRequest) (*pb.Response, error) {
	log.Printf("Received rollback request: term=%d, operation=%s, client=%d",
		in.Term, in.Operation, in.ClientId)

	// Check term
	s.termMutex.Lock()
	if in.Term < s.currentTerm {
		log.Printf("Rejecting rollback: obsolete term %d < our term %d", in.Term, s.currentTerm)
		s.termMutex.Unlock()
		return &pb.Response{Status: pb.Status_LOCK_ERROR, Message: "Rollback from obsolete term"}, nil
	}
	s.termMutex.Unlock()

	// Handle rollback effects
	s.handleLocalRollback(in.Operation, in.ClientId)

	return &pb.Response{Status: pb.Status_SUCCESS}, nil
}

// GetLeader implements the get_leader RPC
func (s *server) GetLeader(ctx context.Context, in *pb.Int) (*pb.LeaderInfo, error) {
	s.termMutex.Lock()
	defer s.termMutex.Unlock()

	// In this phase, server 0 is always the leader
	leaderIdx := s.leaderIndex
	return &pb.LeaderInfo{
		LeaderId:      int32(leaderIdx),
		LeaderAddress: s.serverList[leaderIdx],
	}, nil
}
