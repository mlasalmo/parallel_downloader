package parallel_downloader

import (
	"io"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
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
	err = CreateEmptyFile(tmpFile.Name(), fileSize)
	require.NoError(t, err)

	// Check if the file size matches the expected size
	fileInfo, err := os.Stat(tmpFile.Name())
	require.NoError(t, err)
	require.Equal(t, fileSize, fileInfo.Size())
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
	retries := int(3)
	size := int64(1024)

	// Create a wait group and channel for synchronization
	var wg sync.WaitGroup
	ch := make(chan error)

	wg.Add(1)
	// Call the downloadChunk function
	go DownloadChunk(url, tmpFile, offset, size, retries, &wg, ch)

	// Wait for the download to complete
	wg.Wait()

	// Check if any errors occurred during the download
	select {
	case err := <-ch:
		require.NoError(t, err)
	default:
	}

	// Check if the file size matches the expected size
	fileInfo, err := os.Stat(tmpFile.Name())
	require.NoError(t, err)
	require.Equal(t, size, fileInfo.Size())
}

func TestGetFileHandle(t *testing.T) {
	// Create a temporary file for testing
	tmpFile, err := os.CreateTemp("", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	// Call the getFileHandle function
	fileHandle := GetFileHandle(tmpFile.Name())

	// Check if the file handle is not nil
	require.NotNil(t, fileHandle)

	// Check if the file handle is at the beginning of the file
	offset, err := fileHandle.Seek(0, io.SeekCurrent)
	require.NoError(t, err)
	require.Equal(t, int64(0), offset)
}
