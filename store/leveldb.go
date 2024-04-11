// Package store provides implementations of the diskoque.Store interface for different storage backends.
// The LevelDBStore is a LevelDB-based implementation that stores messages in a LevelDB database.
package store

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/joerodriguez/diskoque"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

// LevelDBStore implements the diskoque.Store interface using LevelDB for storage.
// Messages are stored with unique IDs as keys, allowing efficient retrieval and deletion.
type LevelDBStore struct {
	db *leveldb.DB // The LevelDB database where messages are stored.
}

// NewLevelDB initializes a new LevelDBStore using the provided LevelDB database instance.
// This database instance is used to store, retrieve, and delete messages.
func NewLevelDB(db *leveldb.DB) *LevelDBStore {
	return &LevelDBStore{
		db: db,
	}
}

// Push writes a message to the LevelDB database. It generates a unique ID for each message,
// serializes the message to JSON, and stores it in the database using the ID as the key.
func (f *LevelDBStore) Push(message *diskoque.Message) error {
	// Generate a unique ID for the message
	message.ID = generateID()

	// Marshal the message to JSON for storage
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}

	// Store the serialized message in the database
	return f.db.Put([]byte(message.ID), data, nil)
}

// Iterator creates and returns a LevelDBStoreIterator that can be used to iterate over messages
// stored in the LevelDB database. It enables batch processing of messages by fetching multiple IDs at a time.
func (f *LevelDBStore) Iterator() (diskoque.StoreIterator, error) {
	iterator := f.db.NewIterator(nil, nil)

	return &LevelDBStoreIterator{
		iterator: iterator,
	}, nil
}

// Get retrieves a message by its ID from the LevelDB database, deserializes it from JSON into a diskoque.Message,
// and returns the message object. This allows for efficient message retrieval using its unique ID.
func (f *LevelDBStore) Get(id diskoque.MessageID) (*diskoque.Message, error) {
	value, err := f.db.Get([]byte(id), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get value from db: %w", err)
	}

	msg := &diskoque.Message{}
	err = json.Unmarshal(value, msg)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal value into Message: %w", err)
	}

	return msg, nil
}

// Delete removes a message from the LevelDB database using the message ID as the key.
// This facilitates efficient deletion of messages once they are processed or no longer needed.
func (f *LevelDBStore) Delete(id diskoque.MessageID) error {
	return f.db.Delete([]byte(id), nil)
}

// LevelDBStoreIterator implements the diskoque.StoreIterator interface for LevelDB,
// allowing for the iteration over messages in the database. It supports fetching a specific number
// of message IDs at a time and can be closed to release resources associated with the iterator.
type LevelDBStoreIterator struct {
	iterator iterator.Iterator // The LevelDB iterator used for iterating over the message keys.
}

// NextN fetches the next N message IDs from the LevelDB database, ensuring that only messages
// scheduled for the current time or the past are included. This method enables batch retrieval
// of message IDs for processing.
func (f *LevelDBStoreIterator) NextN(numMessages int) ([]diskoque.MessageID, error) {
	messageIDs := make([]diskoque.MessageID, 0, numMessages)
	for f.iterator.Next() {
		id := string(f.iterator.Key())

		// Skip messages scheduled for the future, and since leveldb iteration is ordered by key, all subsequent
		// messages will also be in the future
		attemptAt, _ := strconv.ParseInt(strings.Split(id, "-")[0], 10, 64)
		if attemptAt > time.Now().UnixNano() {
			break
		}

		messageIDs = append(messageIDs, diskoque.MessageID(id))

		if len(messageIDs) == numMessages {
			break
		}
	}

	if len(messageIDs) == 0 {
		return nil, diskoque.IteratorDone
	}

	return messageIDs, nil
}

// Close releases resources associated with the iterator, such as closing database connections
// or cleaning up internal data structures. It should be called when iteration is complete.
func (f *LevelDBStoreIterator) Close() error {
	f.iterator.Release()
	return f.iterator.Error()
}
