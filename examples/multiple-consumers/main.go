package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/joerodriguez/diskoque"
)

const numMessages = 10000
const numWorkers = 100

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	q, closeQ := diskoque.New(
		"deliveries",
		diskoque.WithDataDirectory("/Users/joerodriguez/data/queues/deliveries"),
		diskoque.WithMaxAttempts(3),
		diskoque.WithExponentialBackoff(10*time.Second, 2*time.Minute),
	)
	defer closeQ()

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

	m := sync.Mutex{}
	eventsProcessed := make(map[string]struct{})
	processed := func(data string) {
		m.Lock()
		defer m.Unlock()

		_, alreadyProcessed := eventsProcessed[data]
		if alreadyProcessed {
			fmt.Println("already processed: " + data)
		}

		eventsProcessed[data] = struct{}{}

		if len(eventsProcessed) == numMessages {
			fmt.Println("all messages processed exactly once")
			go func() {
				time.Sleep(100 * time.Millisecond)
				syscall.Exit(0)
			}()
		}
	}

	wg := sync.WaitGroup{}
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			_ = q.Receive(ctx, func(ctx context.Context, msg *diskoque.Message) error {
				//randomSleep := rand.Intn(101)
				//time.Sleep(time.Duration(randomSleep) * time.Millisecond)
				processed(msg.Data)
				return nil
			})

			wg.Done()
		}()
	}

	waitForSigTerm()
	cancel()

	wg.Wait()
}

func waitForSigTerm() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
}
