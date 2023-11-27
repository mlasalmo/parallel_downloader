package parallel_downloader

import (
	"io"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateEmptyFile(t *testing.T) {
	// Create a temporary file for testing
	tmpFile, err := os.CreateTemp("", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	// Specify the desired file size
	fileSize := int64(1024)

	// Call the createEmptyFile function
	err = createEmptyFile(tmpFile.Name(), fileSize)
	assert.NoError(t, err)

	// Check if the file size matches the expected size
	fileInfo, err := os.Stat(tmpFile.Name())
	assert.NoError(t, err)
	assert.Equal(t, fileSize, fileInfo.Size())
}

func TestDownloadChunk(t *testing.T) {
	// Create a temporary file for testing
	tmpFile, err := os.CreateTemp("", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	// Specify the URL, offset, and size for testing
	url := "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2018-05.parquet"
	offset := int64(0)
	size := int64(1024)

	// Create a wait group and channel for synchronization
	var wg sync.WaitGroup
	ch := make(chan error)

	wg.Add(1)
	// Call the downloadChunk function
	go downloadChunk(url, tmpFile, offset, size, &wg, ch)

	// Wait for the download to complete
	wg.Wait()

	// Check if any errors occurred during the download
	select {
	case err := <-ch:
		assert.NoError(t, err)
	default:
	}

	// Check if the file size matches the expected size
	fileInfo, err := os.Stat(tmpFile.Name())
	assert.NoError(t, err)
	assert.Equal(t, size, fileInfo.Size())
}

func TestGetFileHandle(t *testing.T) {
	// Create a temporary file for testing
	tmpFile, err := os.CreateTemp("", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	// Call the getFileHandle function
	fileHandle := getFileHandle(tmpFile.Name())

	// Check if the file handle is not nil
	assert.NotNil(t, fileHandle)

	// Check if the file handle is at the beginning of the file
	offset, err := fileHandle.Seek(0, io.SeekCurrent)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), offset)
}
