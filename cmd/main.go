package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"sync"

	"github.com/mlasalmo/parallel_downloader"
)

func main() {
	// Command line flags
	url := flag.String("url", "", "URL of the file to download")
	destFile := flag.String("output", "", "Destination file path")
	numGoroutines := flag.Int("goroutines", 4, "Number of downloading goroutines")
	chunkSize := flag.Int64("chunksize", 1024*1024, "Chunk size for parallel downloading")
	flag.Parse()

	// Validate flags
	if *url == "" || *destFile == "" {
		fmt.Println("Usage: go run main.go -url <URL> -output <output_file> [-goroutines <num_goroutines>] [-chunksize <chunk_size>]")
		os.Exit(1)
	}

	// Issue a HEAD request to get file size and MD5 signature
	resp, err := http.Head(*url)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Println("Error:", resp.Status)
		os.Exit(1)
	}

	fileSize := resp.ContentLength
	fileMD5 := resp.Header.Get("ETag")

	// Create an empty file with the required size
	err = createEmptyFile(*destFile, fileSize)
	if err != nil {
		fmt.Println("Error creating empty file:", err)
		os.Exit(1)
	}

	// Download the file in parallel using goroutines
	var wg sync.WaitGroup
	ch := make(chan error, *numGoroutines)

	for i := 0; i < *numGoroutines; i++ {
		start := int64(i) * *chunkSize
		end := start + *chunkSize - 1
		if end >= fileSize {
			end = fileSize - 1
		}
		size := end - start + 1

		wg.Add(1)
		go downloadChunk(*url, getFileHandle(*destFile), start, size, &wg, ch)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// Check for errors
	close(ch)
	for err := range ch {
		if err != nil {
			fmt.Println("Error:", err)
			// Cancel all goroutines on error and delete the file
			_ = os.Remove(*destFile)
			os.Exit(1)
		}
	}

	fmt.Println("Download completed successfully. File MD5:", fileMD5)
}
