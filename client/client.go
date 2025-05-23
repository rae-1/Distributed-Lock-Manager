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

	// Loop interval
	interval := 10 * time.Second

	// Number of iterations
	iterations := 2
	count := 0

	for {
		log.Printf("Starting operation cycle %d", count+1)

		// Acquire the lock
		var acquireRetryCount uint8 = 2
		err = client.RPC_acquire_lock(rpc, acquireRetryCount)
		if err != nil {
			log.Printf("Failed to acquire lock: %v", err)
			// Wait before next attempt
			time.Sleep(interval)
			continue
		}
		time.Sleep(2 * time.Second)

		// Append to a file
		var appendRetryCount uint8 = 2
		fileName := "file_0"
		data := fmt.Sprintf("Data from client %d at %s (cycle %d)\n", rpc.ClientId, time.Now().String(), count+1)
		err = client.RPC_append_file(rpc, fileName, data, appendRetryCount)
		if err != nil {
			log.Printf("Failed to append to file: %v", err)
			// Always try to release the lock even if append failed
			releaseErr := client.RPC_release_lock(rpc, 2)
			if releaseErr != nil {
				log.Printf("Failed to release lock after append error: %v", releaseErr)
			}
			// Wait before next attempt
			time.Sleep(interval)
			continue
		}
		time.Sleep(6 * time.Second)

		// Release the lock
		var releaseRetryCount uint8 = 2
		err = client.RPC_release_lock(rpc, releaseRetryCount)
		if err != nil {
			log.Printf("Failed to release lock: %v", err)
			// Wait before next attempt
			time.Sleep(interval)
			continue
		}

		fmt.Printf("Client operation cycle %d completed successfully\n", count+1)

		count++

		// Break after certain number of iterations (0 means run forever)
		if iterations > 0 && count >= iterations {
			break
		}

		// Wait before next cycle
		log.Printf("Waiting %v before next cycle", interval)
		time.Sleep(interval)
	}

	fmt.Println("Client program completed")
}
