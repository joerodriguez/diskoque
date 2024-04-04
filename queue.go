package diskoque

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync"
	"time"
)

type Queue struct {
	dataDir       string
	unclaimedDir  string
	claimedDir    string
	maxAttempts   uint8
	minRetryDelay time.Duration
	maxRetryDelay time.Duration

	unclaimedChan chan string

	sync.Mutex
	lockedMessages map[string]struct{}
	debugReadFiles map[string]struct{}
}

type QueueOption func(*Queue)
type QueueCloser func()

func New(name string, options ...QueueOption) (*Queue, QueueCloser) {
	q := &Queue{
		dataDir:        fmt.Sprintf("/data/%s", name),
		maxAttempts:    1,
		lockedMessages: make(map[string]struct{}),
		debugReadFiles: make(map[string]struct{}),
		unclaimedChan:  make(chan string),
	}

	for _, option := range options {
		option(q)
	}

	q.unclaimedDir = fmt.Sprintf("%s/unclaimed", q.dataDir)
	q.mustDir(q.unclaimedDir)
	q.claimedDir = fmt.Sprintf("%s/claimed", q.dataDir)
	q.mustDir(q.claimedDir)

	stop := q.startPushingToUnclaimedChan()
	return q, stop
}

func WithDataDirectory(dataDir string) QueueOption {
	return func(q *Queue) {
		q.dataDir = dataDir
	}
}

func WithMaxAttempts(maxAttempts uint8) QueueOption {
	return func(q *Queue) {
		q.maxAttempts = maxAttempts
	}
}

func WithExponentialBackoff(minRetryDelay time.Duration, maxRetryDelay time.Duration) QueueOption {
	return func(q *Queue) {
		q.minRetryDelay = minRetryDelay
		q.maxRetryDelay = maxRetryDelay
	}
}

func (q *Queue) Publish(msg *Message) error {
	// Set default metadata
	id := rand.Uint64()
	msg.Attempt = 1
	msg.NextAttemptAfter = time.Now()

	// Generate a unique filename for the message
	filename := fmt.Sprintf("%s/%d-%d.json", q.unclaimedDir, time.Now().UnixNano(), id)

	// Marshal the message to JSON for storage
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	// Write the file
	return os.WriteFile(filename, data, 0666)
}

func (q *Queue) Receive(ctx context.Context, handler func(context.Context, *Message) error) error {

	claim := func(fileName string) (bool, func()) {
		q.Lock()
		defer q.Unlock()

		_, ok := q.lockedMessages[fileName]

		// message is already being worked
		if ok {
			return false, nil
		}

		q.lockedMessages[fileName] = struct{}{}

		return true, func() {
			q.Lock()
			defer q.Unlock()
			delete(q.lockedMessages, fileName)
		}
	}

	process := func(fileName string) error {

		claimedPath := fmt.Sprintf("%s/%s", q.claimedDir, fileName)
		unclaimedPath := fmt.Sprintf("%s/%s", q.unclaimedDir, fileName)

		err := os.Rename(unclaimedPath, claimedPath)
		if err != nil {
			return err
		}

		// open the file for read/write
		file, err := os.OpenFile(claimedPath, os.O_RDONLY, 0666)
		if err != nil {
			return err
		}

		bytes, err := io.ReadAll(file)
		if err != nil {
			// TODO:
			return err
		}

		err = file.Close()
		if err != nil {
			// TODO:
			return err
		}

		msg := &Message{}
		err = json.Unmarshal(bytes, msg)
		if err != nil {
			// TODO: delete the file and log the anomaly
			return err
		}

		err = handler(ctx, msg)
		if err != nil {
			msg.Attempt = msg.Attempt + 1

			// TODO: implement exponential backoff
			if msg.Attempt <= q.maxAttempts {
				msg.NextAttemptAfter = time.Now().Add(10 * time.Second)

				data, err := json.Marshal(msg)
				if err != nil {
					// TODO:
					return err
				}

				err = os.WriteFile(unclaimedPath, data, 0666)
				if err != nil {
					// TODO:
					return err
				}

				return os.Remove(claimedPath)
			}
		}

		return os.Remove(claimedPath)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case fileName := <-q.unclaimedChan:
			ok, release := claim(fileName)
			// skip if the message is already claimed
			if !ok {
				continue
			}

			err := process(fileName)
			release()
			if err != nil {
				time.Sleep(time.Second)
			}
		}
	}
}

func (q *Queue) mustDir(dir string) {
	err := os.MkdirAll(dir, 0777)
	if err != nil {
		panic(err)
	}
}

func (q *Queue) startPushingToUnclaimedChan() func() {
	stop := make(chan struct{})

	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(1 * time.Second):
			}

			unclaimedDir, err := os.Open(q.unclaimedDir)
			if err != nil {
				fmt.Println(fmt.Errorf("failed to read unclaimedDir: %w", err))
				continue
			}

			for {
				messageFiles, _ := unclaimedDir.Readdirnames(10)

				if len(messageFiles) == 0 {
					break
				}

				for _, fileName := range messageFiles {
					select {
					case <-stop:
						return
					case q.unclaimedChan <- fileName:
					}
				}
			}

			err = unclaimedDir.Close()
			if err != nil {
				fmt.Println(fmt.Errorf("failed to close unclaimedDir: %w", err))
				continue
			}
		}
	}()

	return func() {
		stop <- struct{}{}
	}
}

type Message struct {
	Data             string
	Attempt          uint8
	NextAttemptAfter time.Time
}
