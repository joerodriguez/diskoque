# diskoque

diskoque (pronounced *di · skow · kyoo*) is a high-performance, disk-based queue system for Go applications, designed to manage asynchronous tasks efficiently with minimal overhead. With a focus on simplicity and reliability, diskoque offers multiple storage backend options, including the filesystem and LevelDB, to persist jobs, ensuring your tasks are maintained across restarts with the flexibility to suit different requirements.
<p align="center">
<img src="docs/logo.webp" alt="Logo" width="300" >
</p>

## Features

- **Multiple Storage Backends**: Choose between flat file storage for simplicity or LevelDB for ordered (FIFO) processing and efficient storage management.
- **Disk-based Persistence**: Ensures that your jobs are resilient across application restarts.
- **FIFO and Out-of-Order Processing**: LevelDB backend provides FIFO processing, while the flat file system allows for concurrent, out-of-order task execution.
- **High Performance**: Engineered for speed, with performance optimized for each storage backend.
- **Scalable**: Adjust the number of workers to seamlessly scale with your workload.
- **Retry Mechanism**: Supports retries with exponential back-off, ensuring messages are processed even in case of temporary failures.
- **Simple API**: Easy to integrate with existing Go applications.
- **No External Dependencies (for flat file storage)**: Runs standalone without the need for additional services or databases.
- **Flexible and Lightweight**: Offers a single-process design ideal for straightforward, process-local task management, with optional use of LevelDB for applications requiring ordered processing.

## Getting Started

### Installation

```bash
go get github.com/joerodriguez/diskoque
```

### Usage

Create a new queue, publish messages to it, and start receiving them. The following example uses the LevelDB storage backend for FIFO message processing:

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/joerodriguez/diskoque"
	"github.com/joerodriguez/diskoque/store"
	"github.com/syndtr/goleveldb/leveldb"
)

func main() {
    db, err := leveldb.OpenFile("/path/to/leveldb/db", nil)
    if err != nil {
        log.Fatal(err)
    }
    
    q := diskoque.New(
        diskoque.WithStore(store.NewLevelDB(db)),
        diskoque.WithMaxAttempts(5),
        diskoque.WithExponentialBackoff(1*time.Second, 30*time.Second),
    )
    
    // Publish a message
    err = q.Publish(&diskoque.Message{
        Data: "Hello, World!",
    })
    if err != nil {
        log.Fatalf("Failed to publish message: %v", err)
    }
	
    ctx, cancel := context.WithCancel(context.Background())
	
    // Receive and process messages
    // blocks until the context is done
    err = q.Receive(ctx, func(ctx context.Context, msg *diskoque.Message) error {
        log.Printf("Received message: %s\n", msg.Data)
        cancel()
        return nil
    })
    if err != nil {
        log.Fatalf("Receive error: %v", err)
    }
	
    err = db.Close()
    if err != nil {
        log.Fatalf("failed to close level db: %v", err)
    }
}

```

### Benchmarks

diskoque is designed for fast and efficient operation. Benchmark results showcase the overhead per message with various 
worker counts, demonstrating scalability and performance:

```bash
joerodriguez@Josephs-MacBook-Pro diskoque % go test ./... -bench=. -benchtime=1s      
goos: darwin
goarch: arm64
pkg: github.com/joerodriguez/diskoque
BenchmarkQueue/1_workers-10                   10         103303988 ns/op
BenchmarkQueue/2_workers-10                   10         103118550 ns/op
BenchmarkQueue/4_workers-10                   20          51828981 ns/op
BenchmarkQueue/8_workers-10                   54          23024576 ns/op
BenchmarkQueue/16_workers-10                 195           6372899 ns/op
BenchmarkQueue/32_workers-10                 381           4344950 ns/op
BenchmarkQueue/64_workers-10                 505           2071964 ns/op
BenchmarkQueue/128_workers-10               1236           1667096 ns/op
BenchmarkQueue/256_workers-10               1408            757980 ns/op
BenchmarkQueue/512_workers-10               2713            401034 ns/op
BenchmarkQueue/1024_workers-10              4768            235397 ns/op
BenchmarkQueue/2048_workers-10              8752            135257 ns/op
BenchmarkQueue/4096_workers-10             18918             64447 ns/op
BenchmarkQueue/8192_workers-10             51955             22321 ns/op
```

### Contributing

Contributions are welcome! Feel free to open an issue or submit a pull request.

### License

diskoque is released under the MIT License. See the LICENSE file for more details.
