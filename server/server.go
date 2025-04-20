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
	clientCounter   int32
	fileLock        sync.Mutex // Global lock for all files
	waitQueue       []int32    // Client IDs waiting for the lock
	queueMutex      sync.Mutex // Mutex for the wait queue
	lockHolder      int32      // Current lock holder
	lockTimeout     int32      // Timeout for lock release
	lockAcquireTime time.Time  // When the current lock was acquired
	lockTimerMutex  sync.Mutex // Mutex for lock timer operations
	clientMutex     sync.Mutex // Mutex for creating a new clientID
	// lastHeartbeat          map[int32]time.Time // Track last heartbeat time for each (active) client
	// heartbeatMutex         sync.Mutex          // Mutex for the heartbeat map
	processedRequests      map[int32]int64 // Track processed requests: clientID -> latest successfull write(seq_num)
	processedRequestsMutex sync.Mutex      // Mutex for processed requests
	stateFile              string
	// clients       map[int32]bool    		// Active clients
}

type serverState struct {
	LockHolder        int32           `json:"lock_holder"`
	WaitQueue         []int32         `json:"wait_queue"`
	ProcessedRequests map[int32]int64 `json:"processed_requests"`
	// LastHeartbeat     map[int32]time.Time `json:"last_heartbeat"`
	Epoch int64 `json:"epoch"`
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
	// s.heartbeatMutex.Lock()
	s.processedRequestsMutex.Lock()

	state := serverState{
		LockHolder:        s.lockHolder,
		WaitQueue:         s.waitQueue,
		ProcessedRequests: s.processedRequests,
		// LastHeartbeat:     s.lastHeartbeat,
		Epoch: time.Now().UnixNano(),
	}

	data, err := json.Marshal(state)
	if err == nil {
		ioutil.WriteFile(s.stateFile, data, 0644)
	}

	s.processedRequestsMutex.Unlock()
	// s.heartbeatMutex.Unlock()
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

/*
func (s *server) Heartbeat(ctx context.Context, in *pb.Int) (*pb.Response, error) {
	clientID := in.Rc

	s.heartbeatMutex.Lock()
	s.lastHeartbeat[clientID] = time.Now()
	s.heartbeatMutex.Unlock()

	return &pb.Response{Status: pb.Status_SUCCESS}, nil
}
*/

func (s *server) ClientInit(ctx context.Context, in *pb.Int) (*pb.Int, error) {
	s.clientMutex.Lock()
	s.clientCounter++
	clientID := s.clientCounter
	// s.clients[clientID] = true
	s.clientMutex.Unlock()

	log.Printf("Client initialized with ID: %d", clientID)
	return &pb.Int{Rc: clientID}, nil
}

var loss bool = false

func (s *server) LockAcquire(ctx context.Context, in *pb.LockArgs) (*pb.Response, error) {
	clientID := in.ClientId
	log.Printf("Lock acquire request from client %d", clientID)

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

	// Check if client is already in the wait queue
	for _, id := range s.waitQueue {
		if id == clientID {
			s.queueMutex.Unlock()
			log.Printf("Client %d is already in the wait queue", clientID)
			return &pb.Response{
				Status:  pb.Status_SUCCESS,
				Message: "Already in wait queue",
			}, nil
		}
	}

	// If no one holds the lock, grant it immediately
	if s.lockHolder == 0 {
		s.lockHolder = clientID

		// Set lock acquisition time when granting the lock
		s.lockTimerMutex.Lock()
		s.lockAcquireTime = time.Now()
		s.lockTimerMutex.Unlock()

		s.queueMutex.Unlock()
		log.Printf("Lock granted to client %d", clientID)

		// if loss {
		// 	// Simulate a network loss scenario
		// 	loss = false
		// 	log.Printf("Simulating network loss for client %d", clientID)
		// 	return nil, fmt.Errorf("simulated network loss")
		// }

		return &pb.Response{Status: pb.Status_SUCCESS}, nil
	}

	// Add client to wait queue
	s.waitQueue = append(s.waitQueue, clientID)
	clientWaitIndex := len(s.waitQueue) - 1
	s.queueMutex.Unlock()

	// Wait until this client gets the lock
	for {
		time.Sleep(100 * time.Millisecond)

		s.queueMutex.Lock()
		if s.lockHolder == clientID {
			s.queueMutex.Unlock()

			// s.lockTimerMutex.Lock()
			// s.lockAcquireTime = time.Now()
			// s.lockTimerMutex.Unlock()

			log.Printf("Lock granted to client %d after waiting", clientID)
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

			// Remove client from heartbeat-checking
			// s.heartbeatMutex.Lock()
			// delete(s.lastHeartbeat, clientID)
			// s.heartbeatMutex.Unlock()

			return nil, ctx.Err()
		default:
			// Continue waiting
		}
	}
}

func (s *server) LockRelease(ctx context.Context, in *pb.LockArgs) (*pb.Response, error) {
	clientID := in.ClientId
	log.Printf("Lock release request from client %d", clientID)

	s.queueMutex.Lock()
	defer s.queueMutex.Unlock()

	// Verify the client holds the lock
	if s.lockHolder != clientID {
		log.Printf("Client %d attempted to release a lock it doesn't hold", clientID)
		return &pb.Response{
			Status:  pb.Status_LOCK_ERROR,
			Message: "lock either released or never acquired.",
		}, nil
	}

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

	return &pb.Response{Status: pb.Status_SUCCESS}, nil
}

func (s *server) FileAppend(ctx context.Context, in *pb.FileArgs) (*pb.Response, error) {
	clientID := in.ClientId
	filename := in.Filename
	content := in.Content
	seq_num := in.SeqNum

	log.Printf("File append request from client %d for file %s", clientID, filename)

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

	// Attempt to open and append to the file
	fiilePath := fmt.Sprintf("data/%s", filename)
	file, err := os.OpenFile(fiilePath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Failed to open file %s: %v", filename, err)
		return &pb.Response{Status: pb.Status_FILE_ERROR}, nil
	}
	defer file.Close()

	_, err = file.Write(content)
	if err != nil {
		log.Printf("Failed to append to file %s: %v", filename, err)
		return &pb.Response{Status: pb.Status_FILE_ERROR}, nil
	}

	// Mark this request as processed
	s.processedRequestsMutex.Lock()
	s.processedRequests[clientID] = seq_num
	s.processedRequestsMutex.Unlock()

	log.Printf("Successfully appended to file %s", filename)

	// if loss {
	// 	// Simulate a network loss scenario
	// 	loss = false
	// 	log.Printf("Simulating network loss for client %d", clientID)
	// 	return nil, fmt.Errorf("simulated network loss")
	// } else {
	// 	loss = true
	// }

	return &pb.Response{Status: pb.Status_SUCCESS}, nil
}

func (s *server) ClientClose(ctx context.Context, in *pb.Int) (*pb.Int, error) {
	clientID := in.Rc
	log.Printf("Client %d disconnecting", clientID)

	// s.clientMutex.Lock()
	// delete(s.clients, clientID)
	// s.clientMutex.Unlock()

	// Remove from heartbeat tracking
	// s.heartbeatMutex.Lock()
	// delete(s.lastHeartbeat, clientID)
	// s.heartbeatMutex.Unlock()

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
	createFilesFlag := flag.Bool("create-files", true, "Create data files on startup")
	loadStateFlag := flag.Bool("load-state", false, "Load server state from disk on startup")

	// Parse the flags
	flag.Parse()

	port := 50051
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

	s := grpc.NewServer()
	lockServer := &server{
		clientCounter: 0,
		lockHolder:    0,
		lockTimeout:   30,
		waitQueue:     make([]int32, 0),
		// clients:       make(map[int32]bool),
		// lastHeartbeat:     make(map[int32]time.Time),
		processedRequests: make(map[int32]int64),
		stateFile:         "lock_server_state.json",
	}

	// Attempt to recover state if flag is true
	if *loadStateFlag {
		if data, err := ioutil.ReadFile(lockServer.stateFile); err == nil {
			var state serverState
			if err := json.Unmarshal(data, &state); err == nil {
				lockServer.lockHolder = state.LockHolder
				lockServer.waitQueue = state.WaitQueue
				lockServer.processedRequests = state.ProcessedRequests
				// lockServer.lastHeartbeat = make(map[int32]time.Time)
				// for clientID := range state.LastHeartbeat {
				// 	lockServer.lastHeartbeat[clientID] = time.Now()
				// }
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

	// Start persistence goroutine
	go lockServer.persistState()

	// Starting heartbeat checker goroutine
	// go lockServer.checkHeartbeats()

	// Starting lock timeout checker goroutine
	go lockServer.checkLockTimeout()

	pb.RegisterLockServiceServer(s, lockServer)

	log.Printf("Lock server listening on port %d", port)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

/*
func (s *server) checkHeartbeats() {
	heartbeatTimeout := 5 * time.Second
	ticker := time.NewTicker(1 * time.Second)

	for {
		<-ticker.C
		now := time.Now()

		s.heartbeatMutex.Lock()
		// s.clientMutex.Lock()

		// Check for inactive clients
		for clientID, lastBeat := range s.lastHeartbeat {
			if now.Sub(lastBeat) > heartbeatTimeout {
				log.Printf("Client %d considered inactive (no heartbeat for %v)", clientID, heartbeatTimeout)

				// Handle inactive client, similar to what happens in ctx.Done()
				// Clean up wait queue
				s.queueMutex.Lock()
				for i, id := range s.waitQueue {
					if id == clientID {
						s.waitQueue = append(s.waitQueue[:i], s.waitQueue[i+1:]...)
						break
					}
				}

				// Release lock if client was the holder
				if s.lockHolder == clientID {
					if len(s.waitQueue) > 0 {
						s.lockHolder = s.waitQueue[0]
						s.waitQueue = s.waitQueue[1:]
						log.Printf("Lock transferred to client %d after previous holder inactive", s.lockHolder)
					} else {
						s.lockHolder = 0
						log.Printf("Lock released after holder inactive")
					}
				}
				s.queueMutex.Unlock()

				// Remove client records
				// delete(s.clients, clientID)
				delete(s.lastHeartbeat, clientID)
			}
		}

		// s.clientMutex.Unlock()
		s.heartbeatMutex.Unlock()
	}
}
*/

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
