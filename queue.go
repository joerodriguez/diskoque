// Package diskoque provides a file-based message queue system.
// It enables persistent message queueing by writing messages to disk and
// supports controlled message retries with exponential backoff.
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

// Message represents a message within the diskoque system.
// Data holds the message content, and Attempt tracks the number of attempts
// made to process this message.
type Message struct {
	Data    string
	Attempt uint8
}

// Queue represents a queue in the diskoque system. It manages the lifecycle
// of messages from when they're published to when they're received and
// processed. The queue uses a file-based system to persist messages across
// restarts and failures.
type Queue struct {
	dataDir             string
	unclaimedDir        string
	claimedDir          string
	maxAttempts         uint8
	minRetryDelay       time.Duration
	maxRetryDelay       time.Duration
	maxInFlightMessages int

	unclaimedChan chan string

	sync.RWMutex
	lockedMessages map[string]struct{}
	debugReadFiles map[string]struct{}
}

// QueueOption defines a function signature for options that can be passed to the New function to configure a Queue.
type QueueOption func(*Queue)

// QueueCloser defines a function signature for a function returned by New to stop the queue processing.
type QueueCloser func()

// New initializes a new Queue with the specified name and options. It sets up the necessary directories for the queue
// and starts the internal process for pushing unclaimed messages to be processed. It returns a Queue pointer and a QueueCloser
// function to cleanly stop the queue processing.
func New(name string, options ...QueueOption) *Queue {
	q := &Queue{
		dataDir:             fmt.Sprintf("/data/%s", name),
		maxAttempts:         1,
		maxInFlightMessages: 1,
		lockedMessages:      make(map[string]struct{}),
		debugReadFiles:      make(map[string]struct{}),
		unclaimedChan:       make(chan string),
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

	return q
}

// WithDataDirectory is a QueueOption to specify a custom directory for storing queue data files.
func WithDataDirectory(dataDir string) QueueOption {
	return func(q *Queue) {
		q.dataDir = dataDir
	}
}

// WithMaxAttempts is a QueueOption to set the maximum number of attempts to process a message before it's considered failed.
func WithMaxAttempts(maxAttempts uint8) QueueOption {
	return func(q *Queue) {
		q.maxAttempts = maxAttempts
	}
}

// WithExponentialBackoff is a QueueOption to configure the retry delay for messages using exponential backoff strategy,
// with specified minimum and maximum retry delays. If unspecified, the default retry delay is 0 (immediate).
func WithExponentialBackoff(minRetryDelay time.Duration, maxRetryDelay time.Duration) QueueOption {
	return func(q *Queue) {
		q.minRetryDelay = minRetryDelay
		q.maxRetryDelay = maxRetryDelay
	}
}

// WithMaxInFlightMessages controls the number of messages that can be processed concurrently by the queue.
func WithMaxInFlightMessages(numMessages int) QueueOption {
	return func(q *Queue) {
		q.maxInFlightMessages = numMessages
	}
}

// Publish adds a new message to the queue. It automatically sets the attempt count to 1
// and stores the message in the unclaimed directory for processing.
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

// Receive starts processing messages from the queue. It listens for new or requeued messages,
// processes them using the provided handler function, and manages message retry logic based
// on the handler's success or failure. The process continues until the context is canceled.
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

		q.RLock()
		_, ok := q.lockedMessages[fileName]
		q.RUnlock()

		// message is already being worked
		if ok {
			return false, nil
		}

		q.Lock()
		defer q.Unlock()
		q.lockedMessages[fileName] = struct{}{}

		return true, func() {
			q.Lock()
			defer q.Unlock()
			delete(q.lockedMessages, fileName)
		}
	}

	readMessageFromFile := func(claimedPath string) (*Message, error) {
		file, err := os.OpenFile(claimedPath, os.O_RDONLY, 0666)
		if err != nil {
			return nil, fmt.Errorf("failed to open file: %w", err)
		}

		bytes, err := io.ReadAll(file)
		if err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("failed to read file contents: %w", err)
		}

		err = file.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to close file: %w", err)
		}

		msg := &Message{}
		err = json.Unmarshal(bytes, msg)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal file into Message: %w", err)
		}

		return msg, nil
	}

	// execute the handler and remove the file upon success
	// or requeue the message upon failure
	process := func(fileName string) error {
		claimedPath := fmt.Sprintf("%s/%s", q.claimedDir, fileName)
		unclaimedPath := fmt.Sprintf("%s/%s", q.unclaimedDir, fileName)

		// move the file to claimed directory, so it is not repeatedly written to the unclaimed channel
		err := os.Rename(unclaimedPath, claimedPath)
		if err != nil {
			return fmt.Errorf("failed to move file to the claimed dir: %w", err)
		}

		msg, err := readMessageFromFile(claimedPath)
		if err != nil {
			os.Rename(claimedPath, unclaimedPath)
			return err
		}

		err = handler(ctx, msg)
		if err != nil {
			msg.Attempt = msg.Attempt + 1

			// requeue the message if it has not reached the max attempts
			if msg.Attempt <= q.maxAttempts {
				data, err := json.Marshal(msg)
				if err != nil {
					os.Rename(claimedPath, unclaimedPath)
					return fmt.Errorf("failed to marshal file into Message: %w", err)
				}

				delay := nextAttemptIn(msg)
				unclaimedPath := fmt.Sprintf("%s/%d", q.unclaimedDir, time.Now().Add(delay).UnixNano())
				err = os.WriteFile(unclaimedPath, data, 0666)
				if err != nil {
					os.Rename(claimedPath, unclaimedPath)
					return fmt.Errorf("failed to write next attempt to file: %w", err)
				}
			}
		}

		return os.Remove(claimedPath)
	}

	stopWritingToUnclaimedChan := q.startWritingToUnclaimedChan()

	// consume files from the unclaimed channel
	for {
		select {
		case <-ctx.Done():
			stopWritingToUnclaimedChan()
			return ctx.Err()
		case fileName := <-q.unclaimedChan:
			ok, release := claim(fileName)

			// skip if the message is already claimed
			if !ok {
				continue
			}

			go func() {
				err := process(fileName)
				release()
				if err != nil {
					fmt.Println(err.Error())
				}
			}()
		}
	}
}

// directory listing on the unclaimed directory and push the file names to the unclaimed channel
func (q *Queue) startWritingToUnclaimedChan() func() {
	stop := make(chan struct{})

	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(200 * time.Millisecond):
			}

			unclaimedDir, err := os.Open(q.unclaimedDir)
			if err != nil {
				fmt.Println(fmt.Errorf("failed to read unclaimedDir: %w", err))
				continue
			}

			for {
				q.RLock()
				unutilizedCapacity := q.maxInFlightMessages - len(q.lockedMessages)
				q.RUnlock()

				messageFiles, _ := unclaimedDir.Readdirnames(unutilizedCapacity)

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
