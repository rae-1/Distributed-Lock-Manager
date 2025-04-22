package main

import (
	"fmt"
	lock_client "lock-service/client/client_library"
	"log"
	"sync"
	"time"
)

func main() {
	numClients := 50
	opsPerClient := 10

	start := time.Now()
	var wg sync.WaitGroup

	// Start multiple clients
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientId int) {
			defer wg.Done()

			// Connect to server
			rpc, err := lock_client.RPC_init(1, 50052, "localhost")
			if err != nil {
				log.Printf("Client %d failed to connect: %v", clientId, err)
				return
			}
			defer lock_client.RPC_close(rpc)

			// Perform operations
			for j := 0; j < opsPerClient; j++ {
				// opStart := time.Now()

				// Acquire lock
				err := lock_client.RPC_acquire_lock(rpc, 3)
				if err != nil {
					log.Printf("Client %d: Lock acquisition failed: %v", clientId, err)
					continue
				}

				// Append to file
				err = lock_client.RPC_append_file(rpc, "file_0", fmt.Sprintf("Client %d op %d\n", clientId, j), 3)
				if err != nil {
					log.Printf("Client %d: Append failed: %v", clientId, err)
				}

				// Release lock
				err = lock_client.RPC_release_lock(rpc, 3)
				if err != nil {
					log.Printf("Client %d: Lock release failed: %v", clientId, err)
				}

				// opLatency := time.Since(opStart)
				// log.Printf("Client %d: Operation %d completed in %v", clientId, j, opLatency)
			}
		}(i)
	}

	wg.Wait()
	totalTime := time.Since(start)
	totalOps := numClients * opsPerClient

	fmt.Printf("Completed %d operations in %v\n", totalOps, totalTime)
	fmt.Printf("Throughput: %.2f ops/sec\n", float64(totalOps)/totalTime.Seconds())
}
