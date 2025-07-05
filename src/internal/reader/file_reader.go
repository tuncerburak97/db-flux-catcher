package reader

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

// FileReaderImpl implements FileReader interface
type FileReaderImpl struct {
	pollInterval time.Duration
	lastPosition map[string]int64
	positionMu   sync.RWMutex
	isRunning    bool
	runningMu    sync.RWMutex
}

// NewFileReader creates a new file reader instance
func NewFileReader(pollInterval time.Duration) *FileReaderImpl {
	return &FileReaderImpl{
		pollInterval: pollInterval,
		lastPosition: make(map[string]int64),
		positionMu:   sync.RWMutex{},
		isRunning:    false,
		runningMu:    sync.RWMutex{},
	}
}

// StartReading starts reading the log file continuously
func (r *FileReaderImpl) StartReading(ctx context.Context, filePath string, callback func(lines []string) error) error {
	r.runningMu.Lock()
	if r.isRunning {
		r.runningMu.Unlock()
		return fmt.Errorf("file reader is already running")
	}
	r.isRunning = true
	r.runningMu.Unlock()

	defer func() {
		r.runningMu.Lock()
		r.isRunning = false
		r.runningMu.Unlock()
	}()

	log.Printf("Starting to read log file: %s", filePath)

	// Initialize position if not exists
	r.positionMu.Lock()
	if _, exists := r.lastPosition[filePath]; !exists {
		// Start from end of file for new files
		if size, err := r.GetFileSize(filePath); err == nil {
			r.lastPosition[filePath] = size
		} else {
			r.lastPosition[filePath] = 0
		}
	}
	r.positionMu.Unlock()

	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("Stopping file reader for: %s", filePath)
			return ctx.Err()
		case <-ticker.C:
			if err := r.readNewLines(filePath, callback); err != nil {
				log.Printf("Error reading file %s: %v", filePath, err)
				// Continue reading despite errors
			}
		}
	}
}

// readNewLines reads new lines from the file since last position
func (r *FileReaderImpl) readNewLines(filePath string, callback func(lines []string) error) error {
	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return fmt.Errorf("file does not exist: %s", filePath)
	}

	// Get current file size
	currentSize, err := r.GetFileSize(filePath)
	if err != nil {
		return fmt.Errorf("failed to get file size: %w", err)
	}

	// Get last position
	r.positionMu.RLock()
	lastPos := r.lastPosition[filePath]
	r.positionMu.RUnlock()

	// If file was truncated or rotated, start from beginning
	if currentSize < lastPos {
		log.Printf("File %s was truncated or rotated, starting from beginning", filePath)
		r.positionMu.Lock()
		r.lastPosition[filePath] = 0
		r.positionMu.Unlock()
		lastPos = 0
	}

	// If no new data, return
	if currentSize == lastPos {
		return nil
	}

	// Read new lines
	lines, newPos, err := r.ReadFromPosition(filePath, lastPos)
	if err != nil {
		return fmt.Errorf("failed to read from position: %w", err)
	}

	// Update position
	r.positionMu.Lock()
	r.lastPosition[filePath] = newPos
	r.positionMu.Unlock()

	// Process lines if any
	if len(lines) > 0 {
		if err := callback(lines); err != nil {
			return fmt.Errorf("callback error: %w", err)
		}
	}

	return nil
}

// ReadFromPosition reads file from a specific position
func (r *FileReaderImpl) ReadFromPosition(filePath string, position int64) ([]string, int64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Seek to position
	if _, err := file.Seek(position, io.SeekStart); err != nil {
		return nil, 0, fmt.Errorf("failed to seek to position: %w", err)
	}

	var lines []string
	scanner := bufio.NewScanner(file)

	// Set a larger buffer size for long lines
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			lines = append(lines, line)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, 0, fmt.Errorf("scanner error: %w", err)
	}

	// Get current position
	currentPos, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get current position: %w", err)
	}

	return lines, currentPos, nil
}

// GetFileSize returns the current file size
func (r *FileReaderImpl) GetFileSize(filePath string) (int64, error) {
	info, err := os.Stat(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to stat file: %w", err)
	}
	return info.Size(), nil
}

// WatchFile watches for file changes and returns a channel of new lines
func (r *FileReaderImpl) WatchFile(ctx context.Context, filePath string) (<-chan []string, error) {
	linesChan := make(chan []string, 100) // Buffer for 100 line batches

	go func() {
		defer close(linesChan)

		callback := func(lines []string) error {
			if len(lines) > 0 {
				select {
				case linesChan <- lines:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		}

		if err := r.StartReading(ctx, filePath, callback); err != nil {
			log.Printf("Error in WatchFile: %v", err)
		}
	}()

	return linesChan, nil
}

// GetLastPosition returns the last read position for a file
func (r *FileReaderImpl) GetLastPosition(filePath string) int64 {
	r.positionMu.RLock()
	defer r.positionMu.RUnlock()
	return r.lastPosition[filePath]
}

// SetLastPosition sets the last read position for a file
func (r *FileReaderImpl) SetLastPosition(filePath string, position int64) {
	r.positionMu.Lock()
	defer r.positionMu.Unlock()
	r.lastPosition[filePath] = position
}

// IsRunning returns whether the file reader is currently running
func (r *FileReaderImpl) IsRunning() bool {
	r.runningMu.RLock()
	defer r.runningMu.RUnlock()
	return r.isRunning
}

// Reset resets the file reader state
func (r *FileReaderImpl) Reset() {
	r.positionMu.Lock()
	defer r.positionMu.Unlock()
	r.lastPosition = make(map[string]int64)
}

// GetTrackedFiles returns a list of files being tracked
func (r *FileReaderImpl) GetTrackedFiles() []string {
	r.positionMu.RLock()
	defer r.positionMu.RUnlock()

	files := make([]string, 0, len(r.lastPosition))
	for file := range r.lastPosition {
		files = append(files, file)
	}
	return files
}

// ReadEntireFile reads the entire file content
func (r *FileReaderImpl) ReadEntireFile(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)

	// Set a larger buffer size for long lines
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			lines = append(lines, line)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanner error: %w", err)
	}

	return lines, nil
}

// ReadLastNLines reads the last N lines from a file
func (r *FileReaderImpl) ReadLastNLines(filePath string, n int) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	fileSize := fileInfo.Size()
	if fileSize == 0 {
		return []string{}, nil
	}

	// Start from the end and read backwards
	var lines []string
	var buffer []byte
	var pos int64 = fileSize - 1

	for len(lines) < n && pos >= 0 {
		// Read a chunk
		chunkSize := int64(4096)
		if pos < chunkSize {
			chunkSize = pos + 1
		}

		chunk := make([]byte, chunkSize)
		_, err := file.ReadAt(chunk, pos-chunkSize+1)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read chunk: %w", err)
		}

		// Prepend to buffer
		buffer = append(chunk, buffer...)

		// Split into lines
		content := string(buffer)
		lineList := strings.Split(content, "\n")

		// Keep the last incomplete line in buffer
		if len(lineList) > 1 {
			buffer = []byte(lineList[0])

			// Add complete lines to result (in reverse order)
			for i := len(lineList) - 1; i >= 1; i-- {
				line := strings.TrimSpace(lineList[i])
				if line != "" {
					lines = append(lines, line)
					if len(lines) >= n {
						break
					}
				}
			}
		}

		pos -= chunkSize
	}

	// Reverse the lines to get correct order
	for i := 0; i < len(lines)/2; i++ {
		lines[i], lines[len(lines)-1-i] = lines[len(lines)-1-i], lines[i]
	}

	return lines, nil
}
