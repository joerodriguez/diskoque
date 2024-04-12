package diskoque

import (
	"time"
)

// WithMaxAttempts is a QueueOption to set the maximum number of attempts to process a message before it's considered
// failed.
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
