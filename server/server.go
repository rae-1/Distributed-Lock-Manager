package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	pb "lock-service/lock"

	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedLockServiceServer
	clientCounter int32
	waitQueue     []int32        // Client IDs waiting for the lock
	queueMutex    sync.Mutex     // Mutex for the wait queue
	lockHolder    int32          // Current lock holder
	clients       map[int32]bool // Active clients
	clientMutex   sync.Mutex     // Mutex for the clients map
}

// Spinlock implementation for thread synchronization
type spinlock struct {
	locked uint32
}

func (s *spinlock) acquire() {
	for !s.tryLock() {
		// Spin until lock is acquired
		time.Sleep(1 * time.Millisecond)
	}
}

func (s *spinlock) tryLock() bool {
	return atomic.CompareAndSwapUint32(&s.locked, 0, 1)
}

func (s *spinlock) release() {
	atomic.StoreUint32(&s.locked, 0)
}

// Global spinlock for file access
var globalLock spinlock

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
	// Handle this request in a new goroutine
	responseChan := make(chan *pb.Int)
	errorChan := make(chan error)

	go func() {
		s.clientMutex.Lock()
		s.clientCounter++
		clientID := s.clientCounter
		s.clients[clientID] = true
		s.clientMutex.Unlock()

		log.Printf("Client initialized with ID: %d", clientID)
		responseChan <- &pb.Int{Rc: clientID}
	}()

	// Wait for goroutine to complete or context to expire
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case resp := <-responseChan:
		return resp, nil
	case err := <-errorChan:
		return nil, err
	}
}

func (s *server) LockAcquire(ctx context.Context, in *pb.LockArgs) (*pb.Response, error) {
	clientID := in.ClientId
	log.Printf("Lock acquire request from client %d", clientID)

	// Create channels for the response from the goroutine
	responseChan := make(chan *pb.Response)
	errorChan := make(chan error)
	doneChan := make(chan struct{})

	// Handle lock acquisition in a separate goroutine
	go func() {
		defer close(doneChan)

		s.queueMutex.Lock()

		// If no one holds the lock, grant it immediately
		if s.lockHolder == 0 {
			s.lockHolder = clientID
			s.queueMutex.Unlock()
			log.Printf("Lock granted to client %d", clientID)
			responseChan <- &pb.Response{Status: pb.Status_SUCCESS}
			return
		}

		// Add client to wait queue
		s.waitQueue = append(s.waitQueue, clientID)
		clientWaitIndex := len(s.waitQueue) - 1
		s.queueMutex.Unlock()

		// Use spinlock to wait for the lock
		for {
			time.Sleep(100 * time.Millisecond)

			s.queueMutex.Lock()
			if s.lockHolder == clientID {
				s.queueMutex.Unlock()
				log.Printf("Lock granted to client %d after waiting", clientID)
				responseChan <- &pb.Response{Status: pb.Status_SUCCESS}
				return
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
				errorChan <- ctx.Err()
				return
			default:
				// Continue waiting
			}
		}
	}()

	// Wait for goroutine to complete or context to expire
	select {
	case <-ctx.Done():
		<-doneChan // Wait for goroutine to clean up
		return nil, ctx.Err()
	case resp := <-responseChan:
		return resp, nil
	case err := <-errorChan:
		return nil, err
	}
}

func (s *server) LockRelease(ctx context.Context, in *pb.LockArgs) (*pb.Response, error) {
	clientID := in.ClientId
	log.Printf("Lock release request from client %d", clientID)

	// Create channels for the response from the goroutine
	responseChan := make(chan *pb.Response)
	errorChan := make(chan error)

	// Handle lock release in a separate goroutine
	go func() {
		s.queueMutex.Lock()
		defer s.queueMutex.Unlock()

		// Verify the client holds the lock
		if s.lockHolder != clientID {
			log.Printf("Client %d attempted to release a lock it doesn't hold", clientID)
			responseChan <- &pb.Response{Status: pb.Status_FILE_ERROR}
			return
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

		responseChan <- &pb.Response{Status: pb.Status_SUCCESS}
	}()

	// Wait for goroutine to complete or context to expire
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case resp := <-responseChan:
		return resp, nil
	case err := <-errorChan:
		return nil, err
	}
}

func (s *server) FileAppend(ctx context.Context, in *pb.FileArgs) (*pb.Response, error) {
	clientID := in.ClientId
	filename := in.Filename
	content := in.Content
	filePath := filepath.Join("data", filename)

	log.Printf("File append request from client %d for file %s", clientID, filename)

	// Create channels for the response from the goroutine
	responseChan := make(chan *pb.Response)
	errorChan := make(chan error)

	// Handle file append in a separate goroutine
	go func() {
		// Check if client holds the lock
		s.queueMutex.Lock()
		if s.lockHolder != clientID {
			s.queueMutex.Unlock()
			log.Printf("Client %d attempted to append without holding the lock", clientID)
			responseChan <- &pb.Response{Status: pb.Status_FILE_ERROR}
			return
		}
		s.queueMutex.Unlock()

		// Use global spinlock to ensure exclusive file access
		globalLock.acquire()
		defer globalLock.release()

		// Attempt to open and append to the file
		file, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			log.Printf("Failed to open file %s: %v", filename, err)
			responseChan <- &pb.Response{Status: pb.Status_FILE_ERROR}
			return
		}
		defer file.Close()

		_, err = file.Write(content)
		if err != nil {
			log.Printf("Failed to append to file %s: %v", filename, err)
			responseChan <- &pb.Response{Status: pb.Status_FILE_ERROR}
			return
		}

		log.Printf("Successfully appended to file %s", filename)
		responseChan <- &pb.Response{Status: pb.Status_SUCCESS}
	}()

	// Wait for goroutine to complete or context to expire
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case resp := <-responseChan:
		return resp, nil
	case err := <-errorChan:
		return nil, err
	}
}

func (s *server) ClientClose(ctx context.Context, in *pb.Int) (*pb.Int, error) {
	clientID := in.Rc
	log.Printf("Client %d disconnecting", clientID)

	// Create channels for the response from the goroutine
	responseChan := make(chan *pb.Int)
	errorChan := make(chan error)

	// Handle client disconnection in a separate goroutine
	go func() {
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

		responseChan <- &pb.Int{Rc: 0}
	}()

	// Wait for goroutine to complete or context to expire
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case resp := <-responseChan:
		return resp, nil
	case err := <-errorChan:
		return nil, err
	}
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

	// Create a new gRPC server with multiple concurrent streams
	s := grpc.NewServer(
		grpc.MaxConcurrentStreams(1000), // Allow up to 1000 concurrent requests
	)

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
