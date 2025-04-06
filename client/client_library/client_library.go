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
	Conn          *grpc.ClientConn
	Client        pb.LockServiceClient
	ClientId      int32
	StopHeartbeat func()
	SeqNum        int64
}

// RPC_start_heartbeat starts periodic heartbeats to the server
func RPC_start_heartbeat(rpc *RpcConn) (func(), error) {
	if rpc == nil {
		return nil, fmt.Errorf("rpc connection is nil")
	}

	ticker := time.NewTicker(2 * time.Second)
	stopCh := make(chan struct{})

	go func() {
		for {
			select {
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				_, err := rpc.Client.Heartbeat(ctx, &pb.Int{Rc: rpc.ClientId})
				cancel()
				if err != nil {
					log.Printf("Failed to send heartbeat: %v", err)
				}
			case <-stopCh:
				ticker.Stop()
				return
			}
		}
	}()

	// Return function to stop heartbeat
	return func() {
		close(stopCh)
	}, nil
}

// RPC_init initializes a connection to the server
func RPC_init(srcPort int, dstPort int, dstAddr string) (*RpcConn, error) {
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

	rpcConn := &RpcConn{
		Conn:     conn,
		Client:   client,
		ClientId: clientId,
		SeqNum:   0,
	}

	// Start heartbeat in background
	stopHeartbeat, err := RPC_start_heartbeat(rpcConn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to start heartbeat: %v", err)
	}

	// Store the stopHeartbeat function in the RpcConn
	rpcConn.StopHeartbeat = stopHeartbeat

	return rpcConn, nil
}

// RPC_acquire_lock sends a lock acquire request to the server
func RPC_acquire_lock(rpc *RpcConn) error {
	if rpc == nil {
		return fmt.Errorf("rpc connection is nil")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Printf("Attempting to acquire lock")
	resp, err := rpc.Client.LockAcquire(ctx, &pb.LockArgs{ClientId: rpc.ClientId})
	log.Printf("resp: %v, error: %v", resp, err)
	if err != nil {
		return fmt.Errorf("failed to acquire lock: %v", err)
	}

	if resp.Status != pb.Status_SUCCESS {
		return fmt.Errorf("lock acquisition failed with status: %v", resp.Status)
	}

	log.Printf("Lock acquired successfully")
	return nil
}

// RPC_release_lock sends a lock release request to the server
func RPC_release_lock(rpc *RpcConn) error {
	if rpc == nil {
		return fmt.Errorf("rpc connection is nil")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Printf("Releasing lock")
	resp, err := rpc.Client.LockRelease(ctx, &pb.LockArgs{ClientId: rpc.ClientId})
	if err != nil {
		return fmt.Errorf("failed to release lock: %v", err)
	}

	if resp.Status != pb.Status_SUCCESS {
		return fmt.Errorf("lock release failed with status: %v", resp.Status)
	}

	log.Printf("Lock released successfully")
	return nil
}

// RPC_append_file sends a file append request to the server
func RPC_append_file(rpc *RpcConn, fileName string, data string, appendRetryCount uint8) error {
	if rpc == nil {
		return fmt.Errorf("rpc connection is nil")
	}

	var lastErr error // Store the last error for logging if all attempts fail

	// Retry mechanism for appending to the file
	for attempt := uint8(1); attempt <= appendRetryCount; attempt++ {
		if attempt > 1 {
			// Backoff between retries (100ms, 200ms, 400ms, etc.)
			backoffTime := time.Duration(100*(1<<(attempt-2))) * time.Millisecond
			log.Printf("Retry attempt %d for file append after %v", attempt, backoffTime)
			time.Sleep(backoffTime)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		log.Printf("Appending to file: %s (attempt %d/%d)", fileName, attempt, appendRetryCount)
		resp, err := rpc.Client.FileAppend(ctx, &pb.FileArgs{
			Filename: fileName,
			Content:  []byte(data),
			ClientId: rpc.ClientId,
			SeqNum:   rpc.SeqNum,
		})

		// Store error for potential logging/returning if all attempts fail
		if err != nil {
			lastErr = fmt.Errorf("failed to append to file (attempt %d): %v", attempt, err)
			log.Printf("%v", lastErr)
			cancel()
			continue // Try again
		}

		// Check response status
		if resp.Status != pb.Status_SUCCESS {
			lastErr = fmt.Errorf("file append failed with status: %v (attempt %d)", resp.Status, attempt)
			log.Printf("%v", lastErr)
			cancel()
			continue // Try again
		}

		// Success - increment sequence number and return
		rpc.SeqNum++
		log.Printf("File append successful on attempt %d/%d", attempt, appendRetryCount)
		cancel()
		return nil
	}

	// All attempts failed
	return fmt.Errorf("file append failed after %d attempts: %v", appendRetryCount, lastErr)
}

// RPC_close cleans up the connection to the server
func RPC_close(rpc *RpcConn) error {
	if rpc == nil {
		return fmt.Errorf("rpc connection is nil")
	}

	if rpc.StopHeartbeat != nil {
		rpc.StopHeartbeat()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Printf("Closing connection with server")
	_, err := rpc.Client.ClientClose(ctx, &pb.Int{Rc: rpc.ClientId})
	if err != nil {
		rpc.Conn.Close()
		return fmt.Errorf("failed to close connection: %v", err)
	}

	err = rpc.Conn.Close()
	if err != nil {
		return fmt.Errorf("failed to close gRPC connection: %v", err)
	}

	log.Printf("Connection closed successfully")
	return nil
}
