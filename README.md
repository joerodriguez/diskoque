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
	q := diskoque.New("myQueue",
		diskoque.WithDataDirectory("/path/to/data/directory"),
		diskoque.WithMaxAttempts(5),
		diskoque.WithExponentialBackoff(1*time.Second, 30*time.Second),
	)

	// Publish a message
	err := q.Publish(&diskoque.Message{
		Data: "Hello, World!",
	})
	if err != nil {
		log.Fatalf("Failed to publish message: %v", err)
	}

	// Receive and process messages
	ctx := context.Background()
	err = q.Receive(ctx, func(ctx context.Context, msg *diskoque.Message) error {
		log.Printf("Received message: %s", msg.Data)
		return nil
	})

	if err != nil {
		log.Fatalf("Failed to receive messages: %v", err)
	}
}
```

### Benchmarks

diskoque is designed to be fast. Here are some benchmark results showing the overhead per message with various worker
counts, on an M1 Ultra:
```bash
joerodriguez@Josephs-MacBook-Pro diskoque % go test ./... -bench=. -benchtime=5s
goos: darwin
goarch: arm64
pkg: github.com/joerodriguez/diskoque
BenchmarkQueue/1_workers-10                 1119           4697923 ns/op
BenchmarkQueue/2_workers-10                 1420           4548058 ns/op
BenchmarkQueue/4_workers-10                 1230           4151907 ns/op
BenchmarkQueue/8_workers-10                 2407           2330245 ns/op
BenchmarkQueue/16_workers-10                4266           1521746 ns/op
BenchmarkQueue/32_workers-10                2848           1772116 ns/op
BenchmarkQueue/64_workers-10                5709           1098983 ns/op
BenchmarkQueue/128_workers-10              10000            793299 ns/op
BenchmarkQueue/256_workers-10               5078           1413913 ns/op
BenchmarkQueue/512_workers-10              10000            871647 ns/op
BenchmarkQueue/1024_workers-10              3649           1392767 ns/op
BenchmarkQueue/2048_workers-10              5968           1171895 ns/op
BenchmarkQueue/4096_workers-10              3264           1614191 ns/op
BenchmarkQueue/8192_workers-10              3028           1683323 ns/op
```

### Contributing

Contributions are welcome! Feel free to open an issue or submit a pull request.

### License

diskoque is released under the MIT License. See the LICENSE file for more details.
