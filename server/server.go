package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	pb "lock-service/lock"

	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedLockServiceServer
	clientCounter          int32
	fileLock               sync.Mutex      // Global lock for all files
	waitQueue              []int32         // Client IDs waiting for the lock
	queueMutex             sync.Mutex      // Mutex for the wait queue
	lockHolder             int32           // Current lock holder
	lockTimeout            int32           // Timeout for lock release
	lockAcquireTime        time.Time       // When the current lock was acquired
	lockTimerMutex         sync.Mutex      // Mutex for lock timer operations
	clientMutex            sync.Mutex      // Mutex for creating a new clientID
	processedRequests      map[int32]int64 // Track processed requests: clientID -> latest successful write(seq_num)
	processedRequestsMutex sync.Mutex      // Mutex for processed requests
	stateFile              string

	// Consensus-related fields
	serverId         int32      // This server's ID
	serverList       []string   // List of all server addresses
	leaderIndex      int        // Index of the current leader (-1 if unknown)
	currentTerm      int64      // Current election term
	currentLogNumber int64      // Current log number
	termMutex        sync.Mutex // Mutex for term/log number updates
}

type serverState struct {
	LockHolder        int32           `json:"lock_holder"`
	WaitQueue         []int32         `json:"wait_queue"`
	ProcessedRequests map[int32]int64 `json:"processed_requests"`
	Epoch             int64           `json:"epoch"`

	// Consensus state
	CurrentTerm      int64 `json:"current_term"`
	CurrentLogNumber int64 `json:"current_log_number"`
	LeaderIndex      int   `json:"leader_index"`
}

// save state periodically
func (s *server) persistState() {
	ticker := time.NewTicker(2 * time.Second)
	for {
		<-ticker.C
		s.saveStateToDisk()
	}
}

func (s *server) saveStateToDisk() {
	s.queueMutex.Lock()
	s.processedRequestsMutex.Lock()
	s.termMutex.Lock()

	state := serverState{
		LockHolder:        s.lockHolder,
		WaitQueue:         s.waitQueue,
		ProcessedRequests: s.processedRequests,
		CurrentTerm:       s.currentTerm,
		CurrentLogNumber:  s.currentLogNumber,
		LeaderIndex:       s.leaderIndex,
		// Epoch:             time.Now().UnixNano(),
	}

	data, err := json.Marshal(state)
	if err == nil {
		ioutil.WriteFile(s.stateFile, data, 0644)
	}

	s.termMutex.Unlock()
	s.processedRequestsMutex.Unlock()
	s.queueMutex.Unlock()
}

func createFiles() error {
	dirName := "data"
	if err := os.MkdirAll(dirName, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %v", err)
	}

	// Create 100 files as required
	numFiles := 100
	for i := 0; i < numFiles; i++ {
		fileName := filepath.Join(dirName, fmt.Sprintf("file_%d", i))
		file, err := os.Create(fileName)
		if err != nil {
			return fmt.Errorf("failed to create file %s: %v", fileName, err)
		}
		file.Close()
	}
	return nil
}

func (s *server) ClientInit(ctx context.Context, in *pb.Int) (*pb.Int, error) {
	// Check if we are the leader
	if s.leaderIndex != int(s.serverId) {
		log.Printf("Non-leader server %d redirecting client_init request", s.serverId)
		return &pb.Int{Rc: -1}, fmt.Errorf("not leader, contact %s", s.serverList[s.leaderIndex])
	}

	// Generate client ID as before
	s.clientMutex.Lock()
	s.clientCounter++
	clientID := s.clientCounter
	s.clientMutex.Unlock()

	// Try to replicate to other servers
	_, positiveServers, err := s.ReplicateAndGetConsensus("client_init", clientID, 0)

	if err != nil {
		log.Printf("Failed to replicate client_init: %v", err)

		// Check if we're still the leader after replication attempt
		if s.leaderIndex != int(s.serverId) {
			// We're no longer the leader - redirect client
			log.Printf("Server %d is no longer the leader", s.serverId)
			return &pb.Int{Rc: -1}, fmt.Errorf("leadership changed, contact %s",
				s.serverList[s.leaderIndex])
		}

		// Rollback client counter and notify positive servers
		s.clientMutex.Lock()
		s.clientCounter--
		s.clientMutex.Unlock()

		if len(positiveServers) > 0 {
			s.RollbackOperation(s.currentTerm, "client_init", clientID, positiveServers)
		}

		return nil, fmt.Errorf("failed to initialize client: %v", err)
	}

	log.Printf("Client initialized with ID: %d", clientID)
	return &pb.Int{Rc: clientID}, nil
}

func (s *server) LockAcquire(ctx context.Context, in *pb.LockArgs) (*pb.Response, error) {
	clientID := in.ClientId
	log.Printf("Lock acquire request from client %d", clientID)

	// Check if we are the leader
	if s.leaderIndex != int(s.serverId) {
		log.Printf("Non-leader server %d redirecting lock_acquire request", s.serverId)
		return &pb.Response{
			Status:  pb.Status_REDIRECT,
			Message: "Not the leader server",
			LeaderInfo: &pb.LeaderInfo{
				LeaderId:      int32(s.leaderIndex),
				LeaderAddress: s.serverList[s.leaderIndex],
			},
		}, nil
	}

	s.queueMutex.Lock()

	// Check if client already holds the lock
	if s.lockHolder == clientID {
		s.queueMutex.Unlock()
		log.Printf("Client %d already holds the lock", clientID)
		return &pb.Response{
			Status:  pb.Status_SUCCESS,
			Message: "Already holds the lock",
		}, nil
	}

	// If no one holds the lock, grant it immediately through consensus
	if s.lockHolder == 0 {
		// Release mutex during consensus to avoid blocking
		s.queueMutex.Unlock()

		// Get consensus from other servers
		_, positiveServers, err := s.ReplicateAndGetConsensus("lock_acquire", clientID, 0)

		if err != nil {
			log.Printf("Failed to replicate lock_acquire: %v", err)

			// Check if we're still the leader
			if s.leaderIndex != int(s.serverId) {
				return &pb.Response{
					Status:  pb.Status_REDIRECT,
					Message: "Leadership changed during operation",
					LeaderInfo: &pb.LeaderInfo{
						LeaderId:      int32(s.leaderIndex),
						LeaderAddress: s.serverList[s.leaderIndex],
					},
				}, nil
			}

			// Initiate rollback if any servers approved
			if len(positiveServers) > 0 {
				s.RollbackOperation(s.currentTerm, "lock_acquire", clientID, positiveServers)
			}

			return &pb.Response{
				Status:  pb.Status_LOCK_ERROR,
				Message: fmt.Sprintf("Failed to acquire lock: %v", err),
			}, nil
		}

		log.Printf("Lock granted to client %d", clientID)
		return &pb.Response{Status: pb.Status_SUCCESS}, nil
	}

	// Add client to wait queue (no consensus needed for queue management)
	s.waitQueue = append(s.waitQueue, clientID)
	clientWaitIndex := len(s.waitQueue) - 1
	s.queueMutex.Unlock()

	// Wait until this client gets the lock
	for {
		time.Sleep(100 * time.Millisecond)

		s.queueMutex.Lock()
		if s.lockHolder == clientID {
			s.queueMutex.Unlock()

			// Get consensus from other servers
			_, positiveServers, err := s.ReplicateAndGetConsensus("lock_acquire", clientID, 0)

			if err != nil {
				log.Printf("Failed to replicate lock_acquire: %v", err)

				// Check if we're still the leader
				if s.leaderIndex != int(s.serverId) {
					return &pb.Response{
						Status:  pb.Status_REDIRECT,
						Message: "Leadership changed during operation",
						LeaderInfo: &pb.LeaderInfo{
							LeaderId:      int32(s.leaderIndex),
							LeaderAddress: s.serverList[s.leaderIndex],
						},
					}, nil
				}

				// Initiate rollback if any servers approved
				if len(positiveServers) > 0 {
					s.RollbackOperation(s.currentTerm, "lock_acquire", clientID, positiveServers)
				}

				return &pb.Response{
					Status:  pb.Status_LOCK_ERROR,
					Message: fmt.Sprintf("Failed to acquire lock: %v", err),
				}, nil
			}

			log.Printf("Lock granted to client %d", clientID)
			return &pb.Response{Status: pb.Status_SUCCESS}, nil
		}
		s.queueMutex.Unlock()

		// Check if context is done (client disconnected or timed out)
		select {
		case <-ctx.Done():
			fmt.Printf("client %d disconnected or timed out\n", clientID)

			// Remove client from wait queue
			s.queueMutex.Lock()
			if clientWaitIndex < len(s.waitQueue) {
				s.waitQueue = append(s.waitQueue[:clientWaitIndex], s.waitQueue[clientWaitIndex+1:]...)
			}
			s.queueMutex.Unlock()

			return nil, ctx.Err()
		default:
			// Continue waiting
		}
	}
}

func (s *server) LockRelease(ctx context.Context, in *pb.LockArgs) (*pb.Response, error) {
	clientID := in.ClientId
	log.Printf("Lock release request from client %d", clientID)

	// Check if we are the leader
	if s.leaderIndex != int(s.serverId) {
		log.Printf("Non-leader server %d redirecting lock_release request", s.serverId)
		return &pb.Response{
			Status:  pb.Status_REDIRECT,
			Message: "Not the leader server",
			LeaderInfo: &pb.LeaderInfo{
				LeaderId:      int32(s.leaderIndex),
				LeaderAddress: s.serverList[s.leaderIndex],
			},
		}, nil
	}

	s.queueMutex.Lock()

	// Verify the client holds the lock
	if s.lockHolder != clientID {
		s.queueMutex.Unlock()
		log.Printf("Client %d attempted to release a lock it doesn't hold", clientID)
		return &pb.Response{
			Status:  pb.Status_LOCK_ERROR,
			Message: "lock either released or never acquired.",
		}, nil
	}

	// Release mutex during consensus to avoid blocking
	s.queueMutex.Unlock()

	// Get consensus for the lock release
	_, positiveServers, err := s.ReplicateAndGetConsensus("lock_release", clientID, 0)

	if err != nil {
		log.Printf("Failed to replicate lock_release: %v", err)

		// Check if we're still the leader
		if s.leaderIndex != int(s.serverId) {
			return &pb.Response{
				Status:  pb.Status_REDIRECT,
				Message: "Leadership changed during operation",
				LeaderInfo: &pb.LeaderInfo{
					LeaderId:      int32(s.leaderIndex),
					LeaderAddress: s.serverList[s.leaderIndex],
				},
			}, nil
		}

		// Initiate rollback if any servers approved
		if len(positiveServers) > 0 {
			s.RollbackOperation(s.currentTerm, "lock_release", clientID, positiveServers)
		}

		return &pb.Response{
			Status:  pb.Status_LOCK_ERROR,
			Message: fmt.Sprintf("Failed to release lock: %v", err),
		}, nil
	}

	s.queueMutex.Lock()
	// Release the lock
	if len(s.waitQueue) > 0 {
		// Grant lock to the next client in queue
		s.lockHolder = s.waitQueue[0]
		s.waitQueue = s.waitQueue[1:]

		// Reset lock timer for new holder
		s.lockTimerMutex.Lock()
		s.lockAcquireTime = time.Now()
		s.lockTimerMutex.Unlock()

		log.Printf("Lock transferred to client %d", s.lockHolder)
	} else {
		// No one is waiting, mark lock as free
		s.lockHolder = 0
		log.Printf("Lock released and is now free")
	}
	s.queueMutex.Unlock()

	log.Printf("Lock successfully released by client %d", clientID)
	return &pb.Response{Status: pb.Status_SUCCESS}, nil
}

func (s *server) FileAppend(ctx context.Context, in *pb.FileArgs) (*pb.Response, error) {
	clientID := in.ClientId
	filename := in.Filename
	content := in.Content
	seq_num := in.SeqNum

	log.Printf("File append request from client %d for file %s", clientID, filename)

	// Check if we are the leader
	if s.leaderIndex != int(s.serverId) {
		log.Printf("Non-leader server %d redirecting file_append request", s.serverId)
		return &pb.Response{
			Status:  pb.Status_REDIRECT,
			Message: "Not the leader server",
			LeaderInfo: &pb.LeaderInfo{
				LeaderId:      int32(s.leaderIndex),
				LeaderAddress: s.serverList[s.leaderIndex],
			},
		}, nil
	}

	// Check for duplicate requests (idempotence)
	s.processedRequestsMutex.Lock()
	curSeqNum, exists := s.processedRequests[clientID]
	if exists && curSeqNum >= seq_num {
		s.processedRequestsMutex.Unlock()
		log.Printf("Duplicate request from client %d for file %s", clientID, filename)
		return &pb.Response{
			Status:  pb.Status_SUCCESS,
			Message: "Duplicate request",
		}, nil
	}
	s.processedRequestsMutex.Unlock()

	// Check if client holds the lock
	s.queueMutex.Lock()
	if s.lockHolder != clientID {
		s.queueMutex.Unlock()
		log.Printf("Client %d attempted to append without holding the lock", clientID)
		return &pb.Response{Status: pb.Status_LOCK_ERROR}, nil
	}
	s.queueMutex.Unlock()

	// Get consensus for the file append
	_, positiveServers, err := s.ReplicateAndGetConsensus("file_append", clientID, seq_num)

	if err != nil {
		log.Printf("Failed to replicate file_append: %v", err)

		// Check if we're still the leader
		if s.leaderIndex != int(s.serverId) {
			return &pb.Response{
				Status:  pb.Status_REDIRECT,
				Message: "Leadership changed during operation",
				LeaderInfo: &pb.LeaderInfo{
					LeaderId:      int32(s.leaderIndex),
					LeaderAddress: s.serverList[s.leaderIndex],
				},
			}, nil
		}

		if len(positiveServers) > 0 {
			s.RollbackOperation(s.currentTerm, "lock_release", clientID, positiveServers)
		}
		return &pb.Response{
			Status:  pb.Status_FILE_ERROR,
			Message: fmt.Sprintf("Failed to get consensus for file append: %v", err),
		}, nil
	}

	// Attempt to open and append to the file
	filePath := fmt.Sprintf("data/%s", filename)
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Failed to open file %s: %v", filename, err)

		// Rollback the operation on other servers
		s.RollbackOperation(s.currentTerm, "file_append", clientID, positiveServers)

		return &pb.Response{Status: pb.Status_FILE_ERROR}, nil
	}
	defer file.Close()

	_, err = file.Write(content)
	if err != nil {
		log.Printf("Failed to append to file %s: %v", filename, err)

		// Rollback the operation on other servers
		s.RollbackOperation(s.currentTerm, "file_append", clientID, positiveServers)

		return &pb.Response{Status: pb.Status_FILE_ERROR}, nil
	}

	// Mark this request as processed
	s.processedRequestsMutex.Lock()
	s.processedRequests[clientID] = seq_num
	s.processedRequestsMutex.Unlock()

	log.Printf("Successfully appended to file %s", filename)
	return &pb.Response{Status: pb.Status_SUCCESS}, nil
}

func (s *server) ClientClose(ctx context.Context, in *pb.Int) (*pb.Int, error) {
	clientID := in.Rc
	log.Printf("Client %d disconnecting", clientID)

	// Check if we are the leader
	if s.leaderIndex != int(s.serverId) {
		log.Printf("Non-leader server %d redirecting client_close request", s.serverId)
		return &pb.Int{Rc: -1}, fmt.Errorf("not leader, contact %s", s.serverList[s.leaderIndex])
	}

	// Get consensus for client close
	_, _, err := s.ReplicateAndGetConsensus("client_close", clientID, 0)

	if err != nil {
		log.Printf("Failed to replicate client_close: %v", err)

		// Check if we're still the leader - if not, redirect
		if s.leaderIndex != int(s.serverId) {
			return &pb.Int{Rc: -1}, fmt.Errorf("leadership changed, contact %s",
				s.serverList[s.leaderIndex])
		}

		return nil, fmt.Errorf("failed to close client: %v", err)
	}

	// If the client held the lock, release it
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

	// Remove client from wait queue if present
	for i, id := range s.waitQueue {
		if id == clientID {
			s.waitQueue = append(s.waitQueue[:i], s.waitQueue[i+1:]...)
			break
		}
	}
	s.queueMutex.Unlock()

	return &pb.Int{Rc: 0}, nil
}

func main() {
	// Define command-line flags
	createFilesFlag := flag.Bool("create-files", false, "Create data files on startup")
	loadStateFlag := flag.Bool("load-state", false, "Load server state from disk on startup")
	serverIdFlag := flag.Int("id", 0, "Server ID (0-based index)")
	serverPortFlag := flag.Int("port", 50051, "Server port")

	// Parse the flags
	flag.Parse()

	// Server list - hardcoded for now
	serverList := []string{
		"localhost:50051", // Server 0 (leader)
		"localhost:50052", // Server 1
		"localhost:50053", // Server 2
		"localhost:50054", // Server 3
		"localhost:50055", // Server 4
	}

	serverId := *serverIdFlag
	port := *serverPortFlag

	// Setup listener
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// Create files if flag is true
	if *createFilesFlag {
		// Create 100 files
		if err := createFiles(); err != nil {
			log.Fatalf("Failed to create files: %v", err)
		}
		log.Printf("Created data files")
	} else {
		log.Printf("Skipping file creation")
	}

	// Create server instance with consensus fields
	lockServer := &server{
		clientCounter:     0,
		lockHolder:        0,
		lockTimeout:       30,
		waitQueue:         make([]int32, 0),
		processedRequests: make(map[int32]int64),
		stateFile:         fmt.Sprintf("lock_server_state_%d.json", serverId),

		// Consensus-related fields
		serverId:         int32(serverId),
		serverList:       serverList,
		leaderIndex:      0, // Server 0 is leader for now
		currentTerm:      1, // Start at term 1
		currentLogNumber: 0, // Start with log number 0
	}

	// Attempt to recover state if flag is true
	if *loadStateFlag {
		if data, err := ioutil.ReadFile(lockServer.stateFile); err == nil {
			var state serverState
			if err := json.Unmarshal(data, &state); err == nil {
				lockServer.lockHolder = state.LockHolder
				lockServer.waitQueue = state.WaitQueue
				lockServer.processedRequests = state.ProcessedRequests

				// Recover consensus state
				lockServer.currentTerm = state.CurrentTerm
				lockServer.currentLogNumber = state.CurrentLogNumber
				lockServer.leaderIndex = state.LeaderIndex

				log.Printf("Recovered server state from disk")
			} else {
				log.Printf("Failed to parse state file: %v", err)
			}
		} else {
			log.Printf("No state file found or couldn't read it: %v", err)
		}
	} else {
		log.Printf("Skipping state recovery")
	}

	// Start background tasks
	go lockServer.persistState()
	go lockServer.checkLockTimeout()

	// Register with gRPC
	s := grpc.NewServer()
	pb.RegisterLockServiceServer(s, lockServer)

	log.Printf("Lock server %d listening on port %d (leader: %v)",
		serverId, port, serverId == lockServer.leaderIndex)

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func (s *server) checkLockTimeout() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C

		s.queueMutex.Lock()
		s.lockTimerMutex.Lock()

		// Skip if no one holds the lock
		if s.lockHolder != 0 {
			// Check if the current lock has timed out
			lockHeldFor := time.Since(s.lockAcquireTime)
			if int32(lockHeldFor.Seconds()) > s.lockTimeout {
				expiredClientID := s.lockHolder
				log.Printf("Lock timeout: Client %d held the lock for more than %d seconds", expiredClientID, s.lockTimeout)

				// Move to the next client in the queue
				if len(s.waitQueue) > 0 {
					s.lockHolder = s.waitQueue[0]
					s.waitQueue = s.waitQueue[1:]
					s.lockAcquireTime = time.Now() // Reset timer for new holder
					log.Printf("Lock forcibly transferred to client %d after timeout", s.lockHolder)
				} else {
					s.lockHolder = 0
					log.Printf("Lock forcibly released after timeout")
				}
			}
		}

		s.lockTimerMutex.Unlock()
		s.queueMutex.Unlock()
	}
}
