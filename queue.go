package diskoque

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"sync"
	"time"
)

type Message struct {
	Data    string
	Attempt uint8
}

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

	mustDir := func(dir string) {
		err := os.MkdirAll(dir, 0777)
		if err != nil {
			panic(err)
		}
	}

	q.unclaimedDir = fmt.Sprintf("%s/unclaimed", q.dataDir)
	mustDir(q.unclaimedDir)
	q.claimedDir = fmt.Sprintf("%s/claimed", q.dataDir)
	mustDir(q.claimedDir)

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
	msg.Attempt = 1

	// Generate a unique filename for the message
	filename := fmt.Sprintf("%s/%d", q.unclaimedDir, time.Now().UnixNano())

	// Marshal the message to JSON for storage
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	// Write the file
	return os.WriteFile(filename, data, 0666)
}

func (q *Queue) Receive(ctx context.Context, handler func(context.Context, *Message) error) error {

	// calculate the next attempt delay
	nextAttemptIn := func(msg *Message) time.Duration {
		multiplier := (math.Pow(2, float64(msg.Attempt-1)) - 1) / 2
		delay := time.Duration(multiplier) * q.minRetryDelay
		if delay < q.minRetryDelay {
			delay = q.minRetryDelay
		}
		if delay > q.maxRetryDelay {
			delay = q.maxRetryDelay
		}
		return delay
	}

	// claim a file/message so that it is not processed by another worker
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

	// execute the handler and remove the file upon success
	// or requeue the message upon failure
	process := func(fileName string) error {
		claimedPath := fmt.Sprintf("%s/%s", q.claimedDir, fileName)
		unclaimedPath := fmt.Sprintf("%s/%s", q.unclaimedDir, fileName)

		// move the file to claimed directory, so it is not repeatedly added to the unclaimed channel
		err := os.Rename(unclaimedPath, claimedPath)
		if err != nil {
			return fmt.Errorf("failed to move file to the claimed dir: %w", err)
		}

		file, err := os.OpenFile(claimedPath, os.O_RDONLY, 0666)
		if err != nil {
			return fmt.Errorf("failed to open file: %w", err)
		}

		bytes, err := io.ReadAll(file)
		if err != nil {
			return fmt.Errorf("failed to read file contents: %w", err)
		}

		err = file.Close()
		if err != nil {
			return fmt.Errorf("failed to close file: %w", err)
		}

		msg := &Message{}
		err = json.Unmarshal(bytes, msg)
		if err != nil {
			return fmt.Errorf("failed to unmarshal file into Message: %w", err)
		}

		err = handler(ctx, msg)
		if err != nil {
			msg.Attempt = msg.Attempt + 1

			// requeue the message if it has not reached the max attempts
			if msg.Attempt <= q.maxAttempts {
				data, err := json.Marshal(msg)
				if err != nil {
					return fmt.Errorf("failed to marshal file into Message: %w", err)
				}

				delay := nextAttemptIn(msg)
				unclaimedPath := fmt.Sprintf("%s/%d", q.unclaimedDir, time.Now().Add(delay).UnixNano())
				err = os.WriteFile(unclaimedPath, data, 0666)
				if err != nil {
					return fmt.Errorf("failed to write next attempt to file: %w", err)
				}
			}
		}

		return os.Remove(claimedPath)
	}

	// consume files from the unclaimed channel
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
				fmt.Println(err.Error())
			}
		}
	}
}

// directory listing on the unclaimed directory and push the file names to the unclaimed channel
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

					// if the filename unix nano is in the future, skip it
					attemptAt, _ := strconv.ParseInt(fileName, 10, 64)
					if attemptAt > time.Now().UnixNano() {
						continue
					}

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
