package logging

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type RotatingWriter struct {
	path       string
	maxSize    int64
	maxBackups int
	mu         sync.Mutex
	file       *os.File
	size       int64
}

func NewRotatingWriter(path string, maxSizeMB, maxBackups int) (*RotatingWriter, error) {
	if path == "" {
		return nil, fmt.Errorf("log file path is required")
	}
	if maxSizeMB <= 0 {
		maxSizeMB = 100
	}
	if maxBackups < 0 {
		maxBackups = 0
	}

	writer := &RotatingWriter{
		path:       path,
		maxSize:    int64(maxSizeMB) * 1024 * 1024,
		maxBackups: maxBackups,
	}
	if err := writer.openExisting(); err != nil {
		return nil, err
	}
	return writer, nil
}

func (w *RotatingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		if err := w.openExisting(); err != nil {
			return 0, err
		}
	}

	if w.maxSize > 0 && w.size+int64(len(p)) > w.maxSize {
		if err := w.rotate(); err != nil {
			return 0, err
		}
	}

	n, err := w.file.Write(p)
	w.size += int64(n)
	return n, err
}

func (w *RotatingWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	w.size = 0
	return err
}

func (w *RotatingWriter) openExisting() error {
	dir := filepath.Dir(w.path)
	if dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	file, err := os.OpenFile(w.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return err
	}
	w.file = file
	w.size = info.Size()
	return nil
}

func (w *RotatingWriter) rotate() error {
	if w.file != nil {
		_ = w.file.Close()
		w.file = nil
	}

	if w.maxBackups > 0 {
		for i := w.maxBackups - 1; i >= 1; i-- {
			oldPath := fmt.Sprintf("%s.%d", w.path, i)
			newPath := fmt.Sprintf("%s.%d", w.path, i+1)
			if _, err := os.Stat(oldPath); err == nil {
				_ = os.Rename(oldPath, newPath)
			}
		}
		if _, err := os.Stat(w.path); err == nil {
			_ = os.Rename(w.path, fmt.Sprintf("%s.1", w.path))
		}
	} else {
		if _, err := os.Stat(w.path); err == nil {
			_ = os.Remove(w.path)
		}
	}

	file, err := os.OpenFile(w.path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	w.file = file
	w.size = 0
	return nil
}
