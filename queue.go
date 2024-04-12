// Package diskoque provides a file-based message queue system.
// It enables persistent message queueing by writing messages to disk and
// supports controlled message retries with exponential backoff. It includes
// an integrated circuit breaker to handle high failure rates gracefully by
// temporarily halting operations to allow the system to recover.
package diskoque

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

// MessageID is a string type representing the unique identifier for a message in the queue.
type MessageID string

// Message represents a message within the diskoque system.
// Data holds the message content, and Attempt tracks the number of attempts
// made to process this message.
type Message struct {
	ID            string
	Data          string
	Attempt       uint8
	NextAttemptAt time.Time
}

// Store defines an interface for storage backends that can be used with the diskoque message queue.
// Implementations of Store should handle the persistence of messages and provide methods
// for pushing new messages, iterating over stored messages, retrieving, and deleting messages.
type Store interface {
	Push(*Message) error
	Iterator() (StoreIterator, error)
	Get(MessageID) (*Message, error)
	Delete(MessageID) error
}

// IteratorDone is an error returned by StoreIterator to indicate that there are no more items to iterate over.
var IteratorDone = errors.New("no more items in iterator")

// StoreIterator defines an interface for iterating over messages in the store.
// It allows for batch retrieval of message IDs and should be closed after use to free up resources.
type StoreIterator interface {
	NextN(int) ([]MessageID, error)
	Close() error
}

// CircuitBreaker defines the interface for integrating circuit breaker functionality.
// It allows the queue to manage the flow of message processing based on the health of the service or application.
type CircuitBreaker interface {
	// State returns the current state of the circuit breaker.
	State() CircuitBreakerState
	// Success should be called when a message is successfully processed, indicating healthy operation.
	Success()
	// Failure should be called when a message fails to process, indicating potential issues in operation.
	Failure()
	// ShouldTrial returns true if the circuit breaker should trial process a message a return to normal operation.
	ShouldTrial() bool
}

// CircuitBreakerState represents the possible states of a circuit breaker.
type CircuitBreakerState int

const (
	// CircuitClosed means the Queue will operate normally.
	CircuitClosed CircuitBreakerState = iota
	// CircuitOpen means operation is degraded, publishing or receiving will not take place.
	CircuitOpen
	// RecoveryTrial means a small portion of messages can be received, trialling a return to normal operation.
	// Publishing is still disabled.
	RecoveryTrial
)

// ErrCircuitBreakerOpen is the error returned when an operation is attempted while the circuit breaker is open.
var ErrCircuitBreakerOpen = errors.New("the circuit breaker is open, message can not be published or received at this time")

// Queue represents a queue in the diskoque system. It manages the lifecycle
// of messages from when they're published to when they're received and
// processed. The queue uses a file-based system to persist messages across
// restarts and failures.
type Queue struct {
	// configurable options
	store               Store
	circuitBreaker      CircuitBreaker
	maxAttempts         uint8
	minRetryDelay       time.Duration
	maxRetryDelay       time.Duration
	maxInFlightMessages int

	// internal state for message processing
	unclaimedChan chan MessageID

	// internal state for message locking
	sync.RWMutex
	lockedMessages map[MessageID]struct{}
}

// QueueOption defines a function signature for options that can be passed to the New function to configure a Queue.
type QueueOption func(*Queue)

// New initializes a new Queue with the specified name and options. It sets up the necessary directories for the queue
// and starts the internal process for pushing unclaimed messages to be processed. It returns a Queue pointer.
func New(store Store, circuitBreaker CircuitBreaker, options ...QueueOption) *Queue {
	q := &Queue{
		store:               store,
		circuitBreaker:      circuitBreaker,
		maxAttempts:         1,
		maxInFlightMessages: 1,
		lockedMessages:      make(map[MessageID]struct{}),
		unclaimedChan:       make(chan MessageID),
	}

	for _, option := range options {
		option(q)
	}

	return q
}

// Publish adds a new message to the queue. It automatically sets the attempt count to 1
// and stores the message in the unclaimed directory for processing.
func (q *Queue) Publish(msg *Message) error {
	if q.circuitBreaker.State() != CircuitClosed {
		return ErrCircuitBreakerOpen
	}

	msg.Attempt = 1
	return q.store.Push(msg)
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
	claim := func(id MessageID) (bool, func()) {
		q.Lock()
		defer q.Unlock()

		// message is already being worked
		if _, ok := q.lockedMessages[id]; ok {
			return false, nil
		}

		q.lockedMessages[id] = struct{}{}

		return true, func() {
			q.Lock()
			defer q.Unlock()
			delete(q.lockedMessages, id)
		}
	}

	// execute the handler and delete the message upon success
	// or requeue the message upon failure
	process := func(id MessageID) error {
		msg, err := q.store.Get(id)
		if err != nil {
			return fmt.Errorf("failed to get message from store: %w", err)
		}

		err = handler(ctx, msg)
		if err != nil {
			msg.Attempt = msg.Attempt + 1

			// requeue the message if it has not reached the max attempts
			if msg.Attempt <= q.maxAttempts {
				delay := nextAttemptIn(msg)
				msg.NextAttemptAt = time.Now().Add(delay)

				err = q.store.Push(msg)
				if err != nil {
					return fmt.Errorf("failed to write retry message to store: %w", err)
				}

				err = q.store.Delete(id)
				if err != nil {
					return fmt.Errorf("failed to delete original message from store: %w", err)
				}
			}
		}

		err = q.store.Delete(id)
		if err != nil {
			return fmt.Errorf("failed to delete processed message from store: %w", err)
		}

		return nil
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

// startWritingToUnclaimedChan is an internal helper function that manages reading from the store
// and pushing available messages into the unclaimed channel, respecting the circuit breaker state.
func (q *Queue) startWritingToUnclaimedChan() func() {
	stop := make(chan struct{})

	const MaxMessagesToStartSimultaneously = 500
	numberOfMessagesToProcess := func() int {
		q.RLock()
		unutilizedCapacity := q.maxInFlightMessages - len(q.lockedMessages)
		q.RUnlock()

		if unutilizedCapacity < 0 {
			return 0
		}

		if unutilizedCapacity > MaxMessagesToStartSimultaneously {
			return MaxMessagesToStartSimultaneously
		}

		return unutilizedCapacity
	}

	const downtimeSleepDuration = 200 * time.Millisecond
	go func() {
		timer := time.NewTimer(0)

		for {
			// don't process messages if the circuit breaker is open
			if q.circuitBreaker.State() == CircuitOpen {
				time.Sleep(1 * time.Second)
				continue
			}

			select {
			case <-stop:
				return
			case <-timer.C:
			}

			iterator, err := q.store.Iterator()
			if err != nil {
				break
			}

			for {
				toProcess := numberOfMessagesToProcess()

				// we are at capacity
				if toProcess == 0 {
					timer.Reset(downtimeSleepDuration)
					break
				}

				// there are no more messages to process
				messageIDS, err := iterator.NextN(toProcess)
				if err != nil || len(messageIDS) == 0 {
					timer.Reset(downtimeSleepDuration)
					break
				}

				for _, messageID := range messageIDS {
					// don't process messages if the circuit breaker is open
					if q.circuitBreaker.State() == CircuitOpen {
						break
					}

					select {
					case <-stop:
						return
					default:
					}

					// process a sampling of messages if the circuit breaker is in recovery trial
					if q.circuitBreaker.State() == RecoveryTrial {
						if q.circuitBreaker.ShouldTrial() {
							q.unclaimedChan <- messageID
						}
						continue
					}

					// process the message under normal operation
					q.unclaimedChan <- messageID
				}
			}

			err = iterator.Close()
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
