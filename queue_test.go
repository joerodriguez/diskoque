package diskoque_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/joerodriguez/diskoque"
)

func TestQueue(t *testing.T) {
	t.Run("messages are processed exactly once", func(t *testing.T) {
		const numMessages = 1000
		const numWorkers = 100

		ctx, cancel := context.WithCancel(context.Background())

		// create a temporary directory to store the queue data
		dir, err := os.MkdirTemp("", "diskoque-benchmark")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)

		q := diskoque.New(
			"test-exactly-once",
			diskoque.WithDataDirectory(dir),
			diskoque.WithMaxInFlightMessages(numWorkers),
		)

		go func() {
			for i := 0; i < numMessages; i++ {
				err := q.Publish(&diskoque.Message{
					Data: fmt.Sprintf("message-%d", i),
				})
				if err != nil {
					fmt.Println("err: " + err.Error())
				}
			}
		}()

		success := make(chan struct{})
		m := sync.Mutex{}
		eventsProcessed := make(map[string]struct{})
		processed := func(data string) {
			m.Lock()
			defer m.Unlock()

			_, alreadyProcessed := eventsProcessed[data]
			if alreadyProcessed {
				t.Fatalf("processed same message twice: %s", data)
			}

			eventsProcessed[data] = struct{}{}

			if len(eventsProcessed) == numMessages {
				close(success)
			}
		}

		done := make(chan struct{})
		go func() {
			_ = q.Receive(ctx, func(ctx context.Context, msg *diskoque.Message) error {
				processed(msg.Data)
				return nil
			})

			close(done)
		}()

		<-success

		cancel()
		<-done
	})

	t.Run("messages are retried", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		// create a temporary directory to store the queue data
		dir, err := os.MkdirTemp("", "diskoque-benchmark")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)

		q := diskoque.New(
			"test-retries",
			diskoque.WithDataDirectory(dir),
			diskoque.WithMaxAttempts(2),
			diskoque.WithExponentialBackoff(time.Microsecond, time.Millisecond),
		)

		err = q.Publish(&diskoque.Message{
			Data: "message data",
		})
		if err != nil {
			t.Fatalf(err.Error())
		}

		attempts := atomic.Int64{}
		err = q.Receive(ctx, func(ctx context.Context, msg *diskoque.Message) error {
			attempts.Add(1)
			if msg.Attempt == 1 {
				return errors.New("failed")
			}

			cancel()
			return nil
		})

		if attempts.Load() != 2 {
			t.Fatalf("expected 2 attempts, got %d", attempts.Load())
		}

		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf(err.Error())
		}
	})

	t.Run("in-flight messages who experience unexpected process termination are processed", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		// create a temporary directory to store the queue data
		dir, err := os.MkdirTemp("", "diskoque-benchmark")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)

		// manually create the claimed directory
		claimedDir := fmt.Sprintf("%s/claimed", dir)
		err = os.MkdirAll(claimedDir, 0777)
		if err != nil {
			t.Fatal(err)
		}

		// marshal the message to JSON for storage
		data, err := json.Marshal(&diskoque.Message{
			Data: "message data",
		})
		if err != nil {
			t.Fatal(err)
		}

		// store the message in the claimed directory
		filename := fmt.Sprintf("%s/%d", claimedDir, time.Now().UnixNano())
		err = os.WriteFile(filename, data, 0666)
		if err != nil {
			t.Fatal(err)
		}

		// the message should be processed
		q := diskoque.New(
			"test-restarts",
			diskoque.WithDataDirectory(dir),
		)

		err = q.Receive(ctx, func(ctx context.Context, msg *diskoque.Message) error {
			cancel()
			return nil
		})

		if !errors.Is(err, context.Canceled) {
			t.Fatalf(err.Error())
		}
	})
}

func BenchmarkQueue(b *testing.B) {
	benchmarks := []struct {
		numWorkers int
	}{
		{1},
		{2},
		{4},
		{8},
		{16},
		{32},
		{64},
		{128},
		{256},
		{512},
		{1024},
		{2048},
		{4096},
		{8192},
	}
	for _, bm := range benchmarks {
		b.Run(fmt.Sprintf("%d workers", bm.numWorkers), func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())

			// create a temporary directory to store the queue data
			dir, err := os.MkdirTemp("", "diskoque-benchmark")
			if err != nil {
				b.Fatal(err)
			}
			defer os.RemoveAll(dir)

			q := diskoque.New(
				fmt.Sprintf("benchmark-%d-workers", bm.numWorkers),
				diskoque.WithDataDirectory(dir),
				diskoque.WithMaxInFlightMessages(bm.numWorkers),
			)

			wg := sync.WaitGroup{}
			wg.Add(b.N)

			// add b.N messages to the queue
			go func() {
				for i := 0; i < b.N; i++ {
					_ = q.Publish(&diskoque.Message{
						Data: fmt.Sprintf("message-%d", i),
					})
				}
			}()

			go func() {
				_ = q.Receive(ctx, func(ctx context.Context, msg *diskoque.Message) error {
					wg.Done()
					return nil
				})
			}()

			wg.Wait()

			cancel()
		})
	}
}
