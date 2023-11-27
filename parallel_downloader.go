package parallel_downloader

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"
)

// CreateEmptyFile creates an empty file with the given size
func CreateEmptyFile(path string, size int64) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	file.Seek(size-1, io.SeekStart)
	file.Write([]byte{0})
	return nil
}

// DownloadChunk downloads a portion of the file and writes it to the correct offset in the destination file
func DownloadChunk(url string, destFile *os.File, offset, size int64, retries int, wg *sync.WaitGroup, ch chan<- error) {
	defer wg.Done()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		ch <- err
		return
	}

	rangeHeader := fmt.Sprintf("bytes=%d-%d", offset, offset+size-1)
	req.Header.Set("Range", rangeHeader)

	var resp *http.Response
	var retryCount int

	for retryCount < retries {
		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			retryCount++
			continue
		}
		break
	}

	if err != nil {
		ch <- err
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		ch <- fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		return
	}

	_, err = destFile.Seek(offset, io.SeekStart)
	if err != nil {
		ch <- err
		return
	}

	_, err = io.CopyN(destFile, resp.Body, size)
	if err != nil {
		ch <- err
		return
	}
}

// GetFileHandle opens the file for writing and seeks to the specified offset
func GetFileHandle(path string) *os.File {
	file, err := os.OpenFile(path, os.O_WRONLY, os.ModePerm)
	if err != nil {
		fmt.Println("Error opening file:", err)
		os.Exit(1)
	}
	return file
}
