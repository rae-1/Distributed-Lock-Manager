package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	client "lock-service/client/client_library"
)

var rpc *client.RpcConn
var scanner *bufio.Scanner

func main() {
	scanner = bufio.NewScanner(os.Stdin)
	fmt.Println("Interactive Distributed Lock Manager Client")
	fmt.Println("------------------------------------------")

	for {
		fmt.Println("\nAvailable actions:")
		fmt.Println("1. Initialize client")
		fmt.Println("2. Acquire lock")
		fmt.Println("3. Append to file")
		fmt.Println("4. Release lock")
		fmt.Println("5. Exit")
		fmt.Print("\nChoose an option (1-5): ")

		option := getInput()

		time.Sleep(2 * time.Second) // Simulate some processing time

		switch option {
		case "1":
			initializeClient()
		case "2":
			acquireLock()
		case "3":
			appendToFile()
		case "4":
			releaseLock()
		case "5":
			if rpc != nil {
				client.RPC_close(rpc)
			}
			fmt.Println("Exiting. Goodbye!")
			return
		default:
			fmt.Println("Invalid option. Please choose 1-5.")
		}
	}
}

func getInput() string {
	scanner.Scan()
	return strings.TrimSpace(scanner.Text())
}

func getIntInput(prompt string) int {
	for {
		fmt.Print(prompt)
		input := getInput()
		value, err := strconv.Atoi(input)
		if err != nil {
			fmt.Println("Please enter a valid number.")
			continue
		}
		return value
	}
}

func initializeClient() {
	if rpc != nil {
		fmt.Println("Client is already initialized. Reinitializing...")
		client.RPC_close(rpc)
		rpc = nil
	}

	port := getIntInput("Enter server port (default 50051): ")
	if port == 0 {
		port = 50051
	}

	address := ""
	if address == "" {
		address = "localhost"
	}

	var err error
	rpc, err = client.RPC_init(0, int(port), address)
	if err != nil {
		fmt.Printf("Failed to initialize RPC connection: %v\n", err)
		return
	}

	fmt.Println("Client successfully initialized")
	fmt.Printf("Connected to %s:%d\n", address, port)
}

func acquireLock() {
	if rpc == nil {
		fmt.Println("Client not initialized. Please initialize client first.")
		return
	}

	retryCount := 2
	fmt.Println("Acquiring lock...")
	err := client.RPC_acquire_lock(rpc, uint8(retryCount))
	if err != nil {
		fmt.Printf("Failed to acquire lock: %v\n", err)
		return
	}

	fmt.Println("Lock acquired successfully")
}

func appendToFile() {
	if rpc == nil {
		fmt.Println("Client not initialized. Please initialize client first.")
		return
	}

	fmt.Print("Enter filename(Options: file_0 to file_99): ")
	fileName := getInput()
	if fileName == "" {
		fileName = "file_0"
		fmt.Printf("Using default filename: %s\n", fileName)
	}

	fmt.Print("Enter data to append: ")
	data := getInput()
	if data == "" {
		data = fmt.Sprintf("Data from client %d at %s\n", rpc.ClientId, time.Now().String())
		fmt.Printf("Using default data: %s", data)
	}

	retryCount := 2

	fmt.Println("Appending to file...")
	err := client.RPC_append_file(rpc, fileName, data, uint8(retryCount))
	if err != nil {
		fmt.Printf("Failed to append to file: %v\n", err)
		return
	}

	fmt.Println("Data appended to file successfully")
}

func releaseLock() {
	if rpc == nil {
		fmt.Println("Client not initialized. Please initialize client first.")
		return
	}

	retryCount := 2

	fmt.Println("Releasing lock...")
	err := client.RPC_release_lock(rpc, uint8(retryCount))
	if err != nil {
		fmt.Printf("Failed to release lock: %v\n", err)
		return
	}

	fmt.Println("Lock released successfully")
}
