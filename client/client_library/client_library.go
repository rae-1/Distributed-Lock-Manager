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

	return &RpcConn{
		Conn:     conn,
		Client:   client,
		ClientId: clientId,
	}, nil
}

// RPC_acquire_lock sends a lock acquire request to the server
func RPC_acquire_lock(rpc *RpcConn) error {
	if rpc == nil {
		return fmt.Errorf("rpc connection is nil")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	log.Printf("Attempting to acquire lock")
	resp, err := rpc.Client.LockAcquire(ctx, &pb.LockArgs{ClientId: rpc.ClientId})
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
func RPC_append_file(rpc *RpcConn, fileName string, data string) error {
	if rpc == nil {
		return fmt.Errorf("rpc connection is nil")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Printf("Appending to file: %s", fileName)
	resp, err := rpc.Client.FileAppend(ctx, &pb.FileArgs{
		Filename: fileName,
		Content:  []byte(data),
		ClientId: rpc.ClientId,
	})
	if err != nil {
		return fmt.Errorf("failed to append to file: %v", err)
	}

	if resp.Status != pb.Status_SUCCESS {
		return fmt.Errorf("file append failed with status: %v", resp.Status)
	}

	log.Printf("File append successful")
	return nil
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
