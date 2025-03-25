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

	// // Acquire the lock
	// err = client.RPC_acquire_lock(rpc)
	// if err != nil {
	// 	log.Fatalf("Failed to acquire lock: %v", err)
	// }

	// // Append to a file
	// fileName := "file_0"
	// data := fmt.Sprintf("Data from client %d at %s\n", rpc.ClientId, time.Now().String())
	// err = client.RPC_append_file(rpc, fileName, data)
	// if err != nil {
	// 	log.Fatalf("Failed to append to file: %v", err)
	// }

	// // Add a small delay to simulate work
	// time.Sleep(2 * time.Second)

	// // Release the lock
	// err = client.RPC_release_lock(rpc)
	// if err != nil {
	// 	log.Fatalf("Failed to release lock: %v", err)
	// }

	fmt.Println("Client operation completed successfully")
	time.Sleep(100 * time.Second)
}
