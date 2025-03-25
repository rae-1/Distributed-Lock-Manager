## Installation

1. Install required tools
```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

2. Initialise GO modules
```bash
go mod init lock-service
```

3. Generate the code from proto
```bash
protoc --go_out=. --go-grpc_out=. proto/lock.proto
```

4. Tidy up dependencies
```bash
go mod tidy
```