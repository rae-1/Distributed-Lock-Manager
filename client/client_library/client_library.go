package lock_client

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "lock-service/lock"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RpcConn struct {
	Conn     *grpc.ClientConn
	Client   pb.LockServiceClient
	ClientId int32
	// StopHeartbeat func()
	SeqNum int64

	leaderIdx int
}

var defaultServerList = []string{
	"localhost:50051", // server 0 (initial leader)
	"localhost:50052", // server 1
	"localhost:50053", // server 2
	"localhost:50054", // server 3
	"localhost:50055", // server 4
}

// helper function to check if the error message says to redirect
func isRedirectError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return contains(msg, "contact")
}

func extractLeaderAddr(errMsg string) string {
	const contactStr = "contact"
	idx := -1
	for i := 0; i <= len(errMsg)-len(contactStr); i++ {
		if errMsg[i:i+len(contactStr)] == contactStr {
			idx = i + len(contactStr)
			break
		}
	}

	if idx >= 0 {
		return errMsg[idx:]
	}
	return ""
}

func tryNextServer(serverAddr string) (string, error) {
	for _, addr := range defaultServerList {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}
		client := pb.NewLockServiceClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		resp, err := client.GetLeader(ctx, &pb.Int{Rc: 0})
		if err != nil || resp.LeaderIndex == -1 {
			conn.Close()
			continue
		}

		return resp.LeaderAddress, nil
	}
	return "", fmt.Errorf("Leader not found")
}

// RPC_init initializes a connection to the server
func RPC_init(srcPort int, dstPort int, dstAddr string) (*RpcConn, error) {
	serverAddr := fmt.Sprintf("%s:%d", dstAddr, dstPort)

	// Set up a connection to the server with the given address
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %v", err)
	}

	client := pb.NewLockServiceClient(conn)

	// Initialize the client with the server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Send empty request to get client ID or to know which server is the leader
	resp, err := client.ClientInit(ctx, &pb.Int{Rc: 0})
	if err != nil {
		conn.Close()
		if isRedirectError(err) {
			// Extract the new leader address from the error message and establish a new connection
			leaderAddr := extractLeaderAddr(err.Error())
			// if
			if leaderAddr == "" {
				leaderAddr, err = tryNextServer(serverAddr)
				if err != nil {
					return nil, fmt.Errorf("failed to locate leader: %v", err)
				}
			}
			conn, err = grpc.Dial(leaderAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return nil, fmt.Errorf("failed to connect to new leader: %v", err)
			}

			client = pb.NewLockServiceClient(conn)

			// Retry the initialization with the new connection
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			resp, err = client.ClientInit(ctx, &pb.Int{Rc: 0})
			if err != nil {
				return nil, fmt.Errorf("failed to connect to new leader: %v", err)
			}
		} else {
			return nil, fmt.Errorf("failed to initialize client: %v", err)
		}
	} else {
		return nil, fmt.Errorf("failed to initialize client: %v", err)
	}

	rpcConn := &RpcConn{
		Conn:      conn,
		Client:    pb.NewLockServiceClient(conn),
		ClientId:  resp.Rc,
		leaderIdx: 0,
		SeqNum:    0,
	}

	log.Printf("Connected to server with client ID: %d", resp.Rc)

	/*
		// Start heartbeat in background
		stopHeartbeat, err := RPC_start_heartbeat(rpcConn)
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("failed to start heartbeat: %v", err)
		}

		// Store the stopHeartbeat function in the RpcConn
		rpcConn.StopHeartbeat = stopHeartbeat
	*/

	return rpcConn, nil
}

// Handle server redirect
func handleRedirect(rpc *RpcConn, leaderInfo *pb.LeaderInfo) error {
	if rpc.Conn != nil {
		rpc.Conn.Close()
	}

	newConn, err := grpc.Dial(leaderInfo.LeaderAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to leader %s: %v", leaderInfo.LeaderAddress, err)
	}

	// Update connection
	rpc.Conn = newConn
	rpc.Client = pb.NewLockServiceClient(newConn)
	log.Printf("Redirected to leader %s", leaderInfo.LeaderAddress)

	return nil
}

// RPC_acquire_lock sends a lock acquire request to the server
func RPC_acquire_lock(rpc *RpcConn, acquireRetryCount uint8) error {
	if rpc == nil {
		return fmt.Errorf("rpc connection is nil")
	}

	var lastErr error // Store the last error for logging if all attempts fail

	for attempt := uint8(1); attempt <= acquireRetryCount; attempt++ {
		if attempt > 1 {
			// Exponential backoff between retries (100ms, 200ms, 400ms, etc.)
			backoffTime := time.Duration(100*(1<<(attempt-2))) * time.Millisecond
			log.Printf("Retry attempt %d for lock acquisition after %v", attempt, backoffTime)
			time.Sleep(backoffTime)
			// } else {
			// 	log.Printf("Attempting to acquire lock (attempt %d/%d)", attempt, acquireRetryCount)
			// 	continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)

		log.Printf("Attempting to acquire lock (attempt %d/%d)", attempt, acquireRetryCount)
		resp, err := rpc.Client.LockAcquire(ctx, &pb.LockArgs{ClientId: rpc.ClientId})

		if err != nil {
			lastErr = fmt.Errorf("failed to acquire lock (attempt %d): %v", attempt, err)
			log.Printf("%v", lastErr)
			cancel()
			continue // Try again
		}

		// Handle redirect if we are not talking to the leader
		if resp.Status == pb.Status_REDIRECT {
			log.Printf("Redirected: %s", resp.Message)
			cancel()
			if err := handleRedirect(rpc, resp.LeaderInfo); err != nil {
				log.Printf("Redirect failed: %v", err)
				continue
			}
			continue //Try again with new leader
		}

		if resp.Status == pb.Status_LOCK_ERROR {
			lastErr = fmt.Errorf("lock acquisition failed with status: %v (attempt %d)", resp.Status, attempt)
			log.Printf("%v", lastErr)
			cancel()
			continue // Try again
		}

		// Success - server message lost
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

	var lastErr error // Store the last error for logging if all attempts fail

	// Initial attempt + retries
	for attempt := uint8(1); attempt <= releaseRetryCount; attempt++ {
		if attempt > 1 {
			// Exponential backoff between retries (100ms, 200ms, 400ms, etc.)
			backoffTime := time.Duration(100*(1<<(attempt-2))) * time.Millisecond
			log.Printf("Retry attempt %d for lock release after %v", attempt, backoffTime)
			time.Sleep(backoffTime)
			// } else {
			// 	time.Sleep(2000 * time.Millisecond)
			// 	log.Printf("Attempting to release lock (attempt %d/%d)", attempt, releaseRetryCount)
			// 	continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		log.Printf("Releasing lock (attempt %d/%d)", attempt, releaseRetryCount)
		resp, err := rpc.Client.LockRelease(ctx, &pb.LockArgs{ClientId: rpc.ClientId})

		if err != nil {
			lastErr = fmt.Errorf("failed to release lock (attempt %d): %v", attempt, err)
			log.Printf("%v", lastErr)
			cancel()
			continue // Try again
		}

		if resp.Status == pb.Status_REDIRECT {
			log.Printf("Redirected: %s", resp.Message)
			if err := handleRedirect(rpc, resp.LeaderInfo); err != nil {
				log.Printf("Redirect failed: %v", err)
				continue
			}
			continue // Try again with new leader
		}

		if resp.Status != pb.Status_SUCCESS {
			log.Printf("Lock release message: %s", resp.Message)
			cancel()
			return nil
		}

		log.Printf("Lock released successfully on attempt %d/%d", attempt, releaseRetryCount)
		cancel()
		return nil
	}

	// All attempts failed
	return fmt.Errorf("lock release failed after %d attempts: %v", releaseRetryCount, lastErr)
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

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

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

		if resp.Status == pb.Status_REDIRECT {
			log.Printf("Redirected: %s", resp.Message)
			if err := handleRedirect(rpc, resp.LeaderInfo); err != nil {
				log.Printf("Redirect failed: %v", err)
				continue
			}
			continue // Try again with new leader
		}

		// Check response status
		if resp.Status != pb.Status_SUCCESS {
			if resp.Status == pb.Status_LOCK_ERROR {
				// Client doesn't hold the lock - exit immediately without retry
				cancel()
				return fmt.Errorf("cannot append file: client does not hold the lock")
			}

			// Other errors can be retried
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

	// if rpc.StopHeartbeat != nil {
	// 	rpc.StopHeartbeat()
	// }

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
