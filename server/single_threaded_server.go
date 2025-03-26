package main

import (
	"context"
	"fmt"
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
	clientCounter int32
	fileLock      sync.Mutex     // Global lock for all files
	waitQueue     []int32        // Client IDs waiting for the lock
	queueMutex    sync.Mutex     // Mutex for the wait queue
	lockHolder    int32          // Current lock holder
	clients       map[int32]bool // Active clients
	clientMutex   sync.Mutex     // Mutex for the clients map
}

func createFiles() error {
	dirName := "data"
	if err := os.MkdirAll(dirName, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %v", err)
	}

	// Create 100 files as required
	for i := 0; i < 100; i++ {
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
	s.clientMutex.Lock()
	s.clientCounter++
	clientID := s.clientCounter
	s.clients[clientID] = true
	s.clientMutex.Unlock()

	log.Printf("Client initialized with ID: %d", clientID)
	return &pb.Int{Rc: clientID}, nil
}

func (s *server) LockAcquire(ctx context.Context, in *pb.LockArgs) (*pb.Response, error) {
	clientID := in.ClientId
	log.Printf("Lock acquire request from client %d", clientID)

	s.queueMutex.Lock()

	// If no one holds the lock, grant it immediately
	if s.lockHolder == 0 {
		s.lockHolder = clientID
		s.queueMutex.Unlock()
		log.Printf("Lock granted to client %d", clientID)
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
			log.Printf("Lock granted to client %d after waiting", clientID)
			return &pb.Response{Status: pb.Status_SUCCESS}, nil
		}
		s.queueMutex.Unlock()

		// Check if context is done (client disconnected or timed out)
		select {
		case <-ctx.Done():
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

	s.queueMutex.Lock()
	defer s.queueMutex.Unlock()

	// Verify the client holds the lock
	if s.lockHolder != clientID {
		log.Printf("Client %d attempted to release a lock it doesn't hold", clientID)
		return &pb.Response{Status: pb.Status_FILE_ERROR}, nil
	}

	// Release the lock
	if len(s.waitQueue) > 0 {
		// Grant lock to the next client in queue
		s.lockHolder = s.waitQueue[0]
		s.waitQueue = s.waitQueue[1:]
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

	log.Printf("File append request from client %d for file %s", clientID, filename)

	// Check if client holds the lock
	s.queueMutex.Lock()
	if s.lockHolder != clientID {
		s.queueMutex.Unlock()
		log.Printf("Client %d attempted to append without holding the lock", clientID)
		return &pb.Response{Status: pb.Status_FILE_ERROR}, nil
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

	log.Printf("Successfully appended to file %s", filename)
	return &pb.Response{Status: pb.Status_SUCCESS}, nil
}

func (s *server) ClientClose(ctx context.Context, in *pb.Int) (*pb.Int, error) {
	clientID := in.Rc
	log.Printf("Client %d disconnecting", clientID)

	s.clientMutex.Lock()
	delete(s.clients, clientID)
	s.clientMutex.Unlock()

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
	port := 50051
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// Create the 100 files
	if err := createFiles(); err != nil {
		log.Fatalf("Failed to create files: %v", err)
	}

	s := grpc.NewServer()
	lockServer := &server{
		clientCounter: 0,
		lockHolder:    0,
		waitQueue:     make([]int32, 0),
		clients:       make(map[int32]bool),
	}
	pb.RegisterLockServiceServer(s, lockServer)

	log.Printf("Lock server listening on port %d", port)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
