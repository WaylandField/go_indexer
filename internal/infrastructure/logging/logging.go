package logging

import (
	"io"
	"log"
	"log/slog"
	"os"
	"strings"
)

type Config struct {
	Level      string
	File       string
	MaxSizeMB  int
	MaxBackups int
}

func Init(cfg Config) (*RotatingWriter, error) {
	level := parseLevel(cfg.Level)
	writers := []io.Writer{os.Stdout}

	var rotating *RotatingWriter
	if strings.TrimSpace(cfg.File) != "" {
		writer, err := NewRotatingWriter(cfg.File, cfg.MaxSizeMB, cfg.MaxBackups)
		if err != nil {
			return nil, err
		}
		rotating = writer
		writers = append(writers, writer)
	}

	handler := slog.NewTextHandler(io.MultiWriter(writers...), &slog.HandlerOptions{Level: level})
	logger := slog.New(handler)
	slog.SetDefault(logger)

	stdLogger := slog.NewLogLogger(handler, level)
	log.SetFlags(0)
	log.SetOutput(stdLogger.Writer())

	return rotating, nil
}

func parseLevel(raw string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
