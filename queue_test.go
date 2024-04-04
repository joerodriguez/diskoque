package diskoque_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/joerodriguez/diskoque"
)

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
			)

			wg := sync.WaitGroup{}
			wg.Add(b.N)

			// add b.N messages to the queue
			go func() {
				for i := 0; i < b.N; i++ {
					err = q.Publish(&diskoque.Message{
						Data: fmt.Sprintf("message-%d", i),
					})
					if err != nil {
						// TODO: handle error
					}
				}
			}()

			for i := 0; i < bm.numWorkers; i++ {
				go func() {
					err := q.Receive(ctx, func(ctx context.Context, msg *diskoque.Message) error {
						wg.Done()
						return nil
					})

					if err != nil {
						// TODO: handle error
					}
				}()
			}

			wg.Wait()

			cancel()
		})
	}
}
