package main

import (
	"fmt"
	"log"
	"time"

	client "lock-service/client/client_library"
)

func main() {
	// Connect to the server (on localhost port 50051)
	rpc, err := client.RPC_init(0, 50051, "localhost")
	if err != nil {
		log.Fatalf("Failed to initialize RPC connection: %v", err)
	}
	defer client.RPC_close(rpc)

	// Acquire the lock
	var acquireRetryCount uint8 = 2
	err = client.RPC_acquire_lock(rpc, acquireRetryCount)
	if err != nil {
		log.Fatalf("Failed to acquire lock: %v", err)
	}
	time.Sleep(5 * time.Second)

	// Append to a file
	var appendRetryCount uint8 = 2
	fileName := "file_0"
	data := fmt.Sprintf("Data from client %d at %s\n", rpc.ClientId, time.Now().String())
	err = client.RPC_append_file(rpc, fileName, data, appendRetryCount)
	if err != nil {
		log.Fatalf("Failed to append to file: %v", err)
	}

	// Release the lock
	var releaseRetryCount uint8 = 2
	err = client.RPC_release_lock(rpc, releaseRetryCount)
	if err != nil {
		log.Fatalf("Failed to release lock: %v", err)
	}

	fmt.Println("Client operation completed successfully")
	// time.Sleep(100 * time.Second)
}
