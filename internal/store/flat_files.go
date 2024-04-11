// Package store provides implementations of the diskoque.Store interface for different storage backends.
// The FlatFilesStore is a filesystem-based implementation that stores each message in its own file.
package store

import (
	"encoding/json"
	"fmt"
	"github.com/joerodriguez/diskoque"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// FlatFilesStore implements the diskoque.Store interface using the filesystem.
// Each message is stored in its own file within a specified directory.
type FlatFilesStore struct {
	dataDir string
}

// NewFlatFiles initializes a new FlatFilesStore with the given data directory.
// This directory is used to store all message files.
func NewFlatFiles(dataDir string) *FlatFilesStore {
	return &FlatFilesStore{
		dataDir: dataDir,
	}
}

// Push writes a message to the filesystem as a new file. Each message is stored in its own file,
// with the message ID generated at the time of saving. This ensures each message is uniquely identified
// and can be retrieved or deleted individually.
func (f *FlatFilesStore) Push(message *diskoque.Message) error {
	// Generate a unique filename for the message
	message.ID = generateID()

	// Marshal the message to JSON for storage
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}

	// Write the file
	filename := filepath.Join(f.dataDir, string(message.ID))
	return os.WriteFile(filename, data, 0666)
}

// Iterator returns a StoreIterator that can be used to traverse the messages stored in the filesystem.
// It allows for batch processing of messages by returning a specified number of message IDs at a time.
func (f *FlatFilesStore) Iterator() (diskoque.StoreIterator, error) {
	dir, err := os.Open(f.dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read unclaimedDir: %w", err)
	}

	return &FlatFilesStoreIterator{
		dir: dir,
	}, nil
}

// Get retrieves a message by its ID from the filesystem. It reads the file corresponding to the message ID,
// unmarshals the JSON data into a diskoque.Message, and returns it.
func (f *FlatFilesStore) Get(id diskoque.MessageID) (*diskoque.Message, error) {
	filePath := filepath.Join(f.dataDir, string(id))

	file, err := os.OpenFile(filePath, os.O_RDONLY, 0666)
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

	msg := &diskoque.Message{}
	err = json.Unmarshal(bytes, msg)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal file into Message: %w", err)
	}

	return msg, nil
}

// Delete removes a message file from the filesystem based on the provided message ID.
func (f *FlatFilesStore) Delete(id diskoque.MessageID) error {
	return os.Remove(filepath.Join(f.dataDir, string(id)))
}

// generateID creates a unique ID for each message using the current timestamp and a random number.
// This ensures each message file has a unique filename within the data directory.
func generateID() string {
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), rand.Int64())
}

// FlatFilesStoreIterator implements the diskoque.StoreIterator interface, allowing for batch processing
// of message files within the data directory. It supports fetching a specified number of message IDs at a time
// and closing the iterator when done to free up resources.
type FlatFilesStoreIterator struct {
	dir *os.File
}

// NextN returns the next N message IDs from the iterator. It reads a specified number of filenames from the directory,
// filters out any filenames that represent messages scheduled for the future, and returns the valid message IDs.
func (f *FlatFilesStoreIterator) NextN(numMessages int) ([]diskoque.MessageID, error) {
	messageFiles, _ := f.dir.Readdirnames(numMessages)

	if len(messageFiles) == 0 {
		return nil, diskoque.IteratorDone
	}

	messageIDs := make([]diskoque.MessageID, 0, numMessages)
	for _, fileName := range messageFiles {
		// if the filename unix nano is in the future, skip it
		attemptAt, _ := strconv.ParseInt(strings.Split(fileName, "-")[0], 10, 64)
		if attemptAt > time.Now().UnixNano() {
			continue
		}

		messageIDs = append(messageIDs, diskoque.MessageID(fileName))
	}

	return messageIDs, nil
}

// Close closes the directory file handle, effectively ending the iteration process.
func (f *FlatFilesStoreIterator) Close() error {
	return f.dir.Close()
}
