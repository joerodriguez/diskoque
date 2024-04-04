# diskoque

diskoque is a high-performance, disk-based queue system for Go applications, designed to efficiently manage asynchronous tasks with minimal overhead. With a focus on simplicity and reliability, diskoque leverages the file system to persist jobs, ensuring that your tasks are maintained across restarts without any external dependencies.

<p align="center">
<img src="docs/logo.webp" alt="Logo" width="300" >
</p>

## Features

- **Disk-based Persistence**: Each message is stored as a JSON file, making your jobs resilient across application restarts.
- **Out-of-Order Processing**: Optimized for use cases where tasks can be executed independently and concurrently, without the need for FIFO ordering.
- **High Performance**: Engineered for speed, adding only about 2ms overhead per message with 8 workers.
- **Scalable**: Seamlessly scales with your workload by adjusting the number of workers.
- **Retry Mechanism**: Supports retries with exponential back-off, ensuring messages are processed even in case of temporary failures.
- **Simple API**: Easy to integrate with your existing Go applications.
- **No External Dependencies**: Runs standalone without the need for additional services or databases.
- **Single-Process Design**: Optimized for single-process environments, diskoque utilizes in-process message locking for managing concurrent access to tasks. This design simplifies deployment and reduces the complexity associated with distributed systems, making it an ideal choice for applications that can benefit from straightforward, process-local task management.

## Getting Started

### Installation

```bash
go get github.com/joerodriguez/diskoque
```

### Usage

Create a new queue, publish messages to it, and start receiving them:
```go
package main

import (
	"context"
	"log"
	"time"
	
	"github.com/joerodriguez/diskoque"
)

func main() {
	// Initialize a new queue
	q, closeq := diskoque.New(
		"myQueue",
		diskoque.WithDataDirectory("/path/to/data/directory"),
		diskoque.WithMaxAttempts(5),
		diskoque.WithExponentialBackoff(1*time.Second, 30*time.Second),
	)
	defer closeq()

	// Publish a message
	err := q.Publish(&diskoque.Message{
		Data: "Hello, World!",
	})
	if err != nil {
		log.Fatalf("Failed to publish message: %v", err)
	}

	// Receive and process messages
	ctx, cancel := context.WithCancel(context.Background())
	err = q.Receive(ctx, func(ctx context.Context, msg *diskoque.Message) error {
		log.Printf("Received message: %s", msg.Data)
		return nil
	})

	if err != nil {
		log.Fatalf("Failed to receive messages: %v", err)
	}

	cancel()
}
```

### Benchmarks

diskoque is designed to be fast. Here are some benchmark results showing the overhead per message with various worker
counts, on an M1 Ultra:
```bash
joerodriguez@Josephs-MacBook-Pro diskoque % go test ./... -bench=.
goos: darwin
goarch: arm64
pkg: github.com/joerodriguez/diskoque
BenchmarkQueue/1_workers-10                  199           8537964 ns/op
BenchmarkQueue/2_workers-10                  252           5022640 ns/op
BenchmarkQueue/4_workers-10                  336           5590483 ns/op
BenchmarkQueue/8_workers-10                 1785           1069018 ns/op
BenchmarkQueue/16_workers-10                3412            528523 ns/op
BenchmarkQueue/32_workers-10                2578            518314 ns/op
BenchmarkQueue/64_workers-10                3885            433907 ns/op
BenchmarkQueue/128_workers-10               2358            452674 ns/op
BenchmarkQueue/256_workers-10               2264            444903 ns/op
BenchmarkQueue/512_workers-10               2844            566632 ns/op
BenchmarkQueue/1024_workers-10              2535            534443 ns/op
BenchmarkQueue/2048_workers-10              2227            509665 ns/op
BenchmarkQueue/4096_workers-10              2359            473809 ns/op
BenchmarkQueue/8192_workers-10              1960            575587 ns/op
```

### Contributing

Contributions are welcome! Feel free to open an issue or submit a pull request.

### License

diskoque is released under the MIT License. See the LICENSE file for more details.
