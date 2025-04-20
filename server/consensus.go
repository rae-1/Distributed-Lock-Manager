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

// ReplicateLog handles the replication of log entries to followers (primary-only function)
func (s *server) replicateToServers(entry pb.LogEntry) (bool, error) {
	if !s.isPrimary {
		return false, fmt.Errorf("only primary can replicate logs")
	}

	// Add entry to local log
	s.logMutex.Lock()
	s.currentLogNumber++
	entry.LogNumber = s.currentLogNumber
	entry.Term = s.currentTerm
	s.log = append(s.log, entry)
	logIndex := len(s.log) - 1
	s.logMutex.Unlock()

	// Track success count for majority
	successCount := 1 // Count primary as success
	totalServers := len(s.serverList)
	majority := (totalServers / 2) + 1

	// Channel for responses
	type replicationResult struct {
		success bool
		term    int64
	}
	resultCh := make(chan replicationResult, totalServers-1)

	// Send to all followers
	for i, serverAddr := range s.serverList {
		if i == int(s.serverId) {
			continue // Skip self
		}

		go func(addr string, idx int) {
			// Connect to follower
			conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("Failed to connect to server %s: %v", addr, err)
				resultCh <- replicationResult{false, 0}
				return
			}
			defer conn.Close()

			client := pb.NewLockServiceClient(conn)

			// Send log entry
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			resp, err := client.ReplicateLog(ctx, &entry)

			if err != nil {
				log.Printf("Failed to replicate to server %s: %v", addr, err)
				resultCh <- replicationResult{false, 0}
				return
			}

			// Check if server accepted the entry
			if resp.Success {
				log.Printf("Server %s accepted log entry %d", addr, entry.LogNumber)
				resultCh <- replicationResult{true, resp.Term}
			} else {
				// Server rejected entry (maybe higher term)
				log.Printf("Server %s rejected log entry %d (term %d)", addr, entry.LogNumber, resp.Term)
				resultCh <- replicationResult{false, resp.Term}
			}
		}(serverAddr, i)
	}

	// Wait for responses and check for majority
	for i := 0; i < totalServers-1; i++ {
		result := <-resultCh

		// If we see a higher term, step down as leader
		if result.term > s.currentTerm {
			s.termMutex.Lock()
			s.currentTerm = result.term
			s.isPrimary = false
			s.termMutex.Unlock()

			// Rollback the entry
			s.rollbackLogEntry(entry)

			return false, fmt.Errorf("discovered higher term %d, stepping down", result.term)
		}

		if result.success {
			successCount++
		}

		// Check if we have majority
		if successCount >= majority {
			// Commit the entry
			s.logMutex.Lock()
			s.commitIndex = int64(logIndex)
			s.logMutex.Unlock()

			log.Printf("Log entry %d committed with majority (%d/%d)",
				entry.LogNumber, successCount, totalServers)

			// Apply the entry to state machine
			s.applyLogEntry(entry)

			return true, nil
		}
	}

	// If we get here, we failed to achieve majority
	log.Printf("Failed to achieve majority for log entry %d (%d/%d)",
		entry.LogNumber, successCount, totalServers)

	// Rollback the entry
	s.rollbackLogEntry(entry)

	return false, fmt.Errorf("failed to replicate to majority")
}

// Rollback a log entry after failed consensus
func (s *server) rollbackLogEntry(entry pb.LogEntry) error {
	// First remove from local log
	s.logMutex.Lock()
	if len(s.log) > 0 && s.log[len(s.log)-1].LogNumber == entry.LogNumber {
		s.log = s.log[:len(s.log)-1]
		s.currentLogNumber--
	}
	s.logMutex.Unlock()

	// Send rollback to followers who accepted the entry
	for i, serverAddr := range s.serverList {
		if i == int(s.serverId) {
			continue
		}

		go func(addr string) {
			// Connect to follower
			conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("Failed to connect to server %s for rollback: %v", addr, err)
				return
			}
			defer conn.Close()

			client := pb.NewLockServiceClient(conn)

			// Send rollback request
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			rollbackReq := &pb.RollbackRequest{
				Term:      entry.Term,
				Operation: entry.Operation,
				ClientId:  entry.ClientId,
			}

			_, err = client.Rollback(ctx, rollbackReq)
			if err != nil {
				log.Printf("Failed to rollback on server %s: %v", addr, err)
			}
		}(serverAddr)
	}

	return nil
}

// Apply a committed log entry to the state machine
func (s *server) applyLogEntry(entry pb.LogEntry) {
	switch entry.Operation {
	case "client_init":
		// Client init already happened when primary created the log entry
		log.Printf("Applied client_init for client %d", entry.ClientId)

	case "lock_acquire":
		s.queueMutex.Lock()
		if s.lockHolder == 0 {
			s.lockHolder = entry.ClientId
			s.lockTimerMutex.Lock()
			s.lockAcquireTime = time.Now()
			s.lockTimerMutex.Unlock()
			log.Printf("Applied lock_acquire for client %d", entry.ClientId)
		} else {
			s.waitQueue = append(s.waitQueue, entry.ClientId)
			log.Printf("Added client %d to wait queue", entry.ClientId)
		}
		s.queueMutex.Unlock()

	case "lock_release":
		s.queueMutex.Lock()
		if s.lockHolder == entry.ClientId {
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
		// For followers, just update the processed request counter
		s.processedRequestsMutex.Lock()
		s.processedRequests[entry.ClientId] = entry.SeqNum
		s.processedRequestsMutex.Unlock()
		log.Printf("Updated sequence number for client %d to %d", entry.ClientId, entry.SeqNum)

	case "client_close":
		s.queueMutex.Lock()
		if s.lockHolder == entry.ClientId {
			if len(s.waitQueue) > 0 {
				s.lockHolder = s.waitQueue[0]
				s.waitQueue = s.waitQueue[1:]
				log.Printf("Lock transferred to client %d after previous holder disconnected", s.lockHolder)
			} else {
				s.lockHolder = 0
				log.Printf("Lock released after holder disconnected")
			}
		}

		// Remove client from wait queue if present
		for i, id := range s.waitQueue {
			if id == entry.ClientId {
				s.waitQueue = append(s.waitQueue[:i], s.waitQueue[i+1:]...)
				break
			}
		}
		s.queueMutex.Unlock()
		log.Printf("Applied client_close for client %d", entry.ClientId)
	}
}

// RPC handler for replicate_log
func (s *server) ReplicateLog(ctx context.Context, in *pb.LogEntry) (*pb.LogResponse, error) {
	s.termMutex.Lock()
	defer s.termMutex.Unlock()

	log.Printf("Received log replication: term=%d, log=%d, op=%s, client=%d",
		in.Term, in.LogNumber, in.Operation, in.ClientId)

	// Reject if we have a higher term
	if in.Term < s.currentTerm {
		log.Printf("Rejecting log entry: sender term %d < our term %d", in.Term, s.currentTerm)
		return &pb.LogResponse{
			Term:    s.currentTerm,
			Success: false,
		}, nil
	}

	// Update term if necessary
	if in.Term > s.currentTerm {
		s.currentTerm = in.Term
		s.isPrimary = false // Step down if we were primary
	}

	// Accept the log entry (add to log)
	s.logMutex.Lock()
	// Check if this is the next expected log entry
	if in.LogNumber != s.currentLogNumber+1 {
		s.logMutex.Unlock()
		log.Printf("Rejecting log entry: expected log %d, got %d", s.currentLogNumber+1, in.LogNumber)
		return &pb.LogResponse{
			Term:    s.currentTerm,
			Success: false,
		}, nil
	}

	// Add to log
	s.log = append(s.log, *in)
	s.currentLogNumber = in.LogNumber
	s.logMutex.Unlock()

	// Apply the log entry
	s.applyLogEntry(*in)

	log.Printf("Accepted log entry %d", in.LogNumber)
	return &pb.LogResponse{
		Term:    s.currentTerm,
		Success: true,
	}, nil
}

// RPC handler for rollback
func (s *server) Rollback(ctx context.Context, in *pb.RollbackRequest) (*pb.Response, error) {
	log.Printf("Received rollback request: term=%d, operation=%s, client=%d",
		in.Term, in.Operation, in.ClientId)

	// Check term
	s.termMutex.Lock()
	if in.Term < s.currentTerm {
		s.termMutex.Unlock()
		return &pb.Response{Status: pb.Status_LOCK_ERROR, Message: "Rollback from obsolete term"}, nil
	}
	s.termMutex.Unlock()

	// Remove last log entry
	s.logMutex.Lock()
	if len(s.log) > 0 {
		lastLog := s.log[len(s.log)-1]
		// Only rollback if the operation matches
		if lastLog.Operation == in.Operation && lastLog.ClientId == in.ClientId {
			s.log = s.log[:len(s.log)-1]
			s.currentLogNumber--
		}
	}
	s.logMutex.Unlock()

	// Rollback the operation's effect
	switch in.Operation {
	case "client_init":
		// Decrement client counter
		s.clientMutex.Lock()
		if s.clientCounter > 0 {
			s.clientCounter--
		}
		s.clientMutex.Unlock()

	case "lock_acquire":
		// If this client has the lock, release it
		s.queueMutex.Lock()
		if s.lockHolder == in.ClientId {
			s.lockHolder = 0
		}
		// Remove from wait queue if present
		for i, id := range s.waitQueue {
			if id == in.ClientId {
				s.waitQueue = append(s.waitQueue[:i], s.waitQueue[i+1:]...)
				break
			}
		}
		s.queueMutex.Unlock()

	case "lock_release":
		// Make client the lock holder again if lock is free
		s.queueMutex.Lock()
		if s.lockHolder == 0 {
			s.lockHolder = in.ClientId
			s.lockTimerMutex.Lock()
			s.lockAcquireTime = time.Now()
			s.lockTimerMutex.Unlock()
		}
		s.queueMutex.Unlock()

	case "file_append":
		// Decrement sequence number
		s.processedRequestsMutex.Lock()
		if curSeq, exists := s.processedRequests[in.ClientId]; exists && curSeq > 0 {
			s.processedRequests[in.ClientId]--
		}
		s.processedRequestsMutex.Unlock()

	case "client_close":
		// No action needed
	}

	log.Printf("Rollback for operation %s completed", in.Operation)
	return &pb.Response{Status: pb.Status_SUCCESS}, nil
}

// GetLeader implements the get_leader RPC
func (s *server) GetLeader(ctx context.Context, in *pb.Int) (*pb.LeaderInfo, error) {
	// In this phase, server 0 is always the leader
	return &pb.LeaderInfo{
		LeaderId:      0,
		LeaderAddress: s.serverList[0],
	}, nil
}
