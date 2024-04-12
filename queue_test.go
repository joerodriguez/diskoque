package diskoque_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/joerodriguez/diskoque"
	"github.com/joerodriguez/diskoque/circuitbreaker"
	"github.com/joerodriguez/diskoque/store"
	"github.com/syndtr/goleveldb/leveldb"
)

func TestQueue(t *testing.T) {

	cases := []struct {
		name    string
		factory func() (diskoque.Store, func())
	}{
		{name: "level_db", factory: levelDBStore},
		{name: "flat_files", factory: flatFileStore},
	}
	for _, testCase := range cases {

		t.Run(testCase.name+"/messages are processed exactly once", func(t *testing.T) {
			const numMessages = 1000
			const numWorkers = 100

			ctx, cancel := context.WithCancel(context.Background())

			store, cleanup := testCase.factory()
			defer cleanup()

			q := diskoque.New(
				store,
				circuitbreaker.AlwaysClosed{},
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

		t.Run(testCase.name+"/messages are retried", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())

			store, cleanup := testCase.factory()
			defer cleanup()

			q := diskoque.New(
				store,
				circuitbreaker.AlwaysClosed{},
				diskoque.WithMaxAttempts(2),
				diskoque.WithExponentialBackoff(time.Microsecond, time.Millisecond),
			)

			err := q.Publish(&diskoque.Message{
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
	}

	t.Run("messages can not be published or received if the circuit breaker is open", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		fileStore, cleanup := flatFileStore()
		defer cleanup()

		cb := newFakeCircuitBreaker()
		q := diskoque.New(
			fileStore,
			cb,
		)

		err := q.Publish(&diskoque.Message{
			Data: "message data",
		})
		if err != nil {
			t.Fatalf(err.Error())
		}

		// set the circuit breaker to open
		open := diskoque.CircuitOpen
		cb.state.Store(&open)

		// publishing should fail
		err = q.Publish(&diskoque.Message{
			Data: "message data",
		})
		if !errors.Is(err, diskoque.ErrCircuitBreakerOpen) {
			t.Fatalf("expected circuit breaker open error")
		}

		received := make(chan struct{})
		go func() {
			_ = q.Receive(ctx, func(ctx context.Context, msg *diskoque.Message) error {
				received <- struct{}{}
				return nil
			})
		}()

		// receiving should not work
		select {
		case <-received:
			t.Fatalf("received message when circuit breaker was open")
		case <-time.After(2 * time.Second):
		}

		// set the circuit breaker to recovery trial
		recoveryTrial := diskoque.RecoveryTrial
		cb.state.Store(&recoveryTrial)

		// receiving should still not work, because the circuit breaker is not sampling the message
		select {
		case <-received:
			t.Fatalf("received message when circuit breaker was open")
		case <-time.After(2 * time.Second):
		}

		cb.shouldTrial.Store(true)

		// receiving should work now
		select {
		case <-received:
		case <-time.After(2 * time.Second):
			t.Fatalf("expected messages to be processed under recovery trial")
		}

		cancel()
	})
}

func flatFileStore() (diskoque.Store, func()) {
	// create a temporary directory to store the queue data
	dir, err := os.MkdirTemp("", "diskoque-benchmark")
	if err != nil {
		panic(err)
	}

	return store.NewFlatFiles(dir), func() { os.RemoveAll(dir) }
}

func levelDBStore() (diskoque.Store, func()) {
	// create a temporary directory to store the queue data
	dir, err := os.MkdirTemp("", "diskoque-benchmark")
	if err != nil {
		panic(err)
	}

	db, err := leveldb.OpenFile(filepath.Join(dir, "db"), nil)
	if err != nil {
		panic(err)
	}

	return store.NewLevelDB(db), func() { os.RemoveAll(dir) }
}

func newFakeCircuitBreaker() *fakeCircuitBreaker {
	state := diskoque.CircuitClosed
	atomicPointer := atomic.Pointer[diskoque.CircuitBreakerState]{}
	atomicPointer.Store(&state)

	shouldTrial := atomic.Bool{}
	shouldTrial.Store(false)

	return &fakeCircuitBreaker{
		state:        atomicPointer,
		shouldTrial:  shouldTrial,
		successCount: atomic.Int32{},
	}
}

type fakeCircuitBreaker struct {
	state        atomic.Pointer[diskoque.CircuitBreakerState]
	shouldTrial  atomic.Bool
	successCount atomic.Int32
}

func (f *fakeCircuitBreaker) State() diskoque.CircuitBreakerState {
	return *f.state.Load()
}

func (f *fakeCircuitBreaker) Success() {
	f.successCount.Add(1)
}

func (f *fakeCircuitBreaker) Failure() {}

func (f *fakeCircuitBreaker) ShouldTrial() bool {
	return f.shouldTrial.Load()
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

			db, err := leveldb.OpenFile(filepath.Join(dir, "db"), nil)
			if err != nil {
				b.Fatal(err)
			}

			q := diskoque.New(
				store.NewLevelDB(db),
				circuitbreaker.AlwaysClosed{},
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
