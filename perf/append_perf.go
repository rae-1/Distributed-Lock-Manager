package main

import (
	"fmt"
	lock_client "lock-service/client/client_library"
	"log"
	"sync"
	"time"
)

func main() {
	// Configuration
	numClients := 1                 // Number of concurrent clients
	appendsPerLock := 100           // Number of append operations per lock acquisition
	serverPort := 50051             // Server port to connect to
	serverAddr := "localhost"       // Server address
	var acquireRetryCount uint8 = 3 // Max retries for lock acquisition
	var appendRetryCount uint8 = 3  // Max retries for append operations
	var releaseRetryCount uint8 = 3 // Max retries for lock release

	fmt.Printf("Testing append performance with %d clients, %d appends per lock\n",
		numClients, appendsPerLock)

	start := time.Now()
	var wg sync.WaitGroup
	successfulAppends := 0
	totalAppendTime := time.Duration(0)
	var appendsMutex sync.Mutex

	// Start multiple clients
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientId int) {
			defer wg.Done()

			// Connect to server
			rpc, err := lock_client.RPC_init(0, serverPort, serverAddr)
			if err != nil {
				log.Printf("Client %d failed to connect: %v", clientId, err)
				return
			}
			defer lock_client.RPC_close(rpc)

			// Acquire lock
			err = lock_client.RPC_acquire_lock(rpc, acquireRetryCount)
			if err != nil {
				log.Printf("Client %d: Lock acquisition failed: %v", clientId, err)
				return
			}
			log.Printf("Client %d: Lock acquired", clientId)

			// Perform multiple append operations (only these are timed for performance)
			clientSuccessfulAppends := 0
			clientAppendTime := time.Duration(0)

			for j := 0; j < appendsPerLock; j++ {
				data := fmt.Sprintf("Client %d append %d at %s\n",
					clientId, j, time.Now().Format(time.RFC3339Nano))

				// Time just the append operation
				appendStart := time.Now()
				err = lock_client.RPC_append_file(rpc, "file_0", data, appendRetryCount)
				appendTime := time.Since(appendStart)

				if err != nil {
					log.Printf("Client %d: Append %d failed: %v", clientId, j, err)
					continue
				}

				clientSuccessfulAppends++
				clientAppendTime += appendTime
			}

			// Update global stats with this client's results
			appendsMutex.Lock()
			successfulAppends += clientSuccessfulAppends
			totalAppendTime += clientAppendTime
			appendsMutex.Unlock()

			// Calculate append throughput for this client
			if clientSuccessfulAppends > 0 {
				appendThroughput := float64(clientSuccessfulAppends) / clientAppendTime.Seconds()
				avgAppendLatency := clientAppendTime.Milliseconds() / int64(clientSuccessfulAppends)
				log.Printf("Client %d: Completed %d appends in %v (%.2f appends/sec, avg %d ms/append)",
					clientId, clientSuccessfulAppends, clientAppendTime, appendThroughput, avgAppendLatency)
			}

			// Release lock (not counted in append time)
			err = lock_client.RPC_release_lock(rpc, releaseRetryCount)
			if err != nil {
				log.Printf("Client %d: Lock release failed: %v", clientId, err)
				return
			}
			log.Printf("Client %d: Lock released", clientId)
		}(i)
	}

	wg.Wait()
	totalTime := time.Since(start)

	// Calculate and print results
	fmt.Println("\n===== Append Performance Test Results =====")
	fmt.Printf("Total test duration: %v\n", totalTime)
	fmt.Printf("Completed %d successful appends\n", successfulAppends)

	if successfulAppends > 0 {
		avgAppendTime := float64(totalAppendTime.Milliseconds()) / float64(successfulAppends)
		appendThroughput := float64(successfulAppends) / totalAppendTime.Seconds()

		fmt.Printf("Append operations only:\n")
		fmt.Printf("  Total append time: %v\n", totalAppendTime)
		fmt.Printf("  Average time per append: %.2f ms\n", avgAppendTime)
		fmt.Printf("  Append throughput: %.2f appends/sec\n", appendThroughput)
	}
}
