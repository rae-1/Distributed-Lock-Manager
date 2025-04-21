package lock_client

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "lock-service/lock"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RpcConn struct {
	Conn     *grpc.ClientConn
	Client   pb.LockServiceClient
	ClientId int32
	SeqNum   int64

	// Added server list and current leader tracking
	serverList  []string
	leaderIndex int
}

// Default server list
var defaultServerList = []string{
	"localhost:50051", // Server 0 (initial leader)
	"localhost:50052", // Server 1
	"localhost:50053", // Server 2
	"localhost:50054", // Server 3
	"localhost:50055", // Server 4
}

// RPC_init initializes a connection to the server
func RPC_init(srcPort int, dstPort int, dstAddr string) (*RpcConn, error) {
	// Use specified address or default to localhost
	if dstAddr == "" {
		dstAddr = "localhost"
	}

	serverAddr := fmt.Sprintf("%s:%d", dstAddr, dstPort)

	// Set up a connection to the server
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %v", err)
	}

	client := pb.NewLockServiceClient(conn)

	// Initialize the client with the server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Send empty request to get client ID
	resp, err := client.ClientInit(ctx, &pb.Int{Rc: 0})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to initialize client: %v", err)
	}

	clientId := resp.Rc
	log.Printf("Connected to server with client ID: %d", clientId)

	// Create RPC connection with server list and default leader
	rpcConn := &RpcConn{
		Conn:        conn,
		Client:      client,
		ClientId:    clientId,
		SeqNum:      0,
		serverList:  defaultServerList,
		leaderIndex: 0, // Assume server 0 is leader by default
	}

	return rpcConn, nil
}

// RPC_acquire_lock sends a lock acquire request to the server
func RPC_acquire_lock(rpc *RpcConn, acquireRetryCount uint8) error {
	if rpc == nil {
		return fmt.Errorf("rpc connection is nil")
	}

	var lastErr error

	for attempt := uint8(1); attempt <= acquireRetryCount; attempt++ {
		if attempt > 1 {
			backoffTime := time.Duration(100*(1<<(attempt-2))) * time.Millisecond
			log.Printf("Retry attempt %d for lock acquisition after %v", attempt, backoffTime)
			time.Sleep(backoffTime)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		log.Printf("Attempting to acquire lock (attempt %d/%d)", attempt, acquireRetryCount)
		resp, err := rpc.Client.LockAcquire(ctx, &pb.LockArgs{ClientId: rpc.ClientId})

		if err != nil {
			lastErr = fmt.Errorf("failed to acquire lock (attempt %d): %v", attempt, err)
			log.Printf("%v", lastErr)
			cancel()
			continue
		}

		// Handle response status - now checking for REDIRECT status too
		if resp.Status == pb.Status_REDIRECT {
			log.Printf("Received redirect response: %s", resp.Message)
			// Just log for now - we'll handle leader changes in future updates
			cancel()
			continue
		} else if resp.Status != pb.Status_SUCCESS {
			lastErr = fmt.Errorf("lock acquisition failed with status: %v (attempt %d)", resp.Status, attempt)
			log.Printf("%v", lastErr)
			cancel()
			continue
		}

		if resp.Message != "" {
			log.Printf("Lock acquisition message: %s", resp.Message)
		}

		log.Printf("Lock acquired successfully on attempt %d/%d", attempt, acquireRetryCount)
		cancel()
		return nil
	}

	return fmt.Errorf("lock acquisition failed after %d attempts: %v", acquireRetryCount, lastErr)
}

// RPC_release_lock sends a lock release request to the server
func RPC_release_lock(rpc *RpcConn, releaseRetryCount uint8) error {
	if rpc == nil {
		return fmt.Errorf("rpc connection is nil")
	}

	var lastErr error

	for attempt := uint8(1); attempt <= releaseRetryCount; attempt++ {
		if attempt > 1 {
			backoffTime := time.Duration(100*(1<<(attempt-2))) * time.Millisecond
			log.Printf("Retry attempt %d for lock release after %v", attempt, backoffTime)
			time.Sleep(backoffTime)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		log.Printf("Releasing lock (attempt %d/%d)", attempt, releaseRetryCount)
		resp, err := rpc.Client.LockRelease(ctx, &pb.LockArgs{ClientId: rpc.ClientId})

		if err != nil {
			lastErr = fmt.Errorf("failed to release lock (attempt %d): %v", attempt, err)
			log.Printf("%v", lastErr)
			cancel()
			continue
		}

		// Handle response status - now checking for REDIRECT status too
		if resp.Status == pb.Status_REDIRECT {
			log.Printf("Received redirect response: %s", resp.Message)
			// Just log for now - we'll handle leader changes in future updates
			cancel()
			continue
		} else if resp.Status != pb.Status_SUCCESS {
			// Special case - if lock was already released, that's OK
			if resp.Status == pb.Status_LOCK_ERROR &&
				resp.Message == "lock either released or never acquired." {
				log.Printf("Lock already released or never acquired")
				cancel()
				return nil
			}

			lastErr = fmt.Errorf("lock release failed with status: %v (attempt %d)", resp.Status, attempt)
			log.Printf("%v", lastErr)
			cancel()
			continue
		}

		log.Printf("Lock released successfully on attempt %d/%d", attempt, releaseRetryCount)
		cancel()
		return nil
	}

	return fmt.Errorf("lock release failed after %d attempts: %v", releaseRetryCount, lastErr)
}

// RPC_append_file sends a file append request to the server
func RPC_append_file(rpc *RpcConn, fileName string, data string, appendRetryCount uint8) error {
	if rpc == nil {
		return fmt.Errorf("rpc connection is nil")
	}

	var lastErr error

	for attempt := uint8(1); attempt <= appendRetryCount; attempt++ {
		if attempt > 1 {
			backoffTime := time.Duration(100*(1<<(attempt-2))) * time.Millisecond
			log.Printf("Retry attempt %d for file append after %v", attempt, backoffTime)
			time.Sleep(backoffTime)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		log.Printf("Appending to file: %s (attempt %d/%d)", fileName, attempt, appendRetryCount)
		resp, err := rpc.Client.FileAppend(ctx, &pb.FileArgs{
			Filename: fileName,
			Content:  []byte(data),
			ClientId: rpc.ClientId,
			SeqNum:   rpc.SeqNum,
		})

		if err != nil {
			lastErr = fmt.Errorf("failed to append to file (attempt %d): %v", attempt, err)
			log.Printf("%v", lastErr)
			cancel()
			continue
		}

		// Handle response status - now checking for REDIRECT status too
		if resp.Status == pb.Status_REDIRECT {
			log.Printf("Received redirect response: %s", resp.Message)
			// Just log for now - we'll handle leader changes in future updates
			cancel()
			continue
		} else if resp.Status == pb.Status_LOCK_ERROR {
			// Client doesn't hold the lock - exit immediately without retry
			cancel()
			return fmt.Errorf("cannot append file: client does not hold the lock")
		} else if resp.Status != pb.Status_SUCCESS {
			lastErr = fmt.Errorf("file append failed with status: %v (attempt %d)", resp.Status, attempt)
			log.Printf("%v", lastErr)
			cancel()
			continue
		}

		// Success - increment sequence number and return
		rpc.SeqNum++
		log.Printf("File append successful on attempt %d/%d", attempt, appendRetryCount)
		cancel()
		return nil
	}

	return fmt.Errorf("file append failed after %d attempts: %v", appendRetryCount, lastErr)
}

// RPC_close cleans up the connection to the server
func RPC_close(rpc *RpcConn) error {
	if rpc == nil {
		return fmt.Errorf("rpc connection is nil")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Printf("Closing connection with server")
	_, err := rpc.Client.ClientClose(ctx, &pb.Int{Rc: rpc.ClientId})
	if err != nil {
		log.Printf("Error during client_close: %v", err)
		// Continue to close the connection anyway
	}

	err = rpc.Conn.Close()
	if err != nil {
		return fmt.Errorf("failed to close gRPC connection: %v", err)
	}

	log.Printf("Connection closed successfully")
	return nil
}
