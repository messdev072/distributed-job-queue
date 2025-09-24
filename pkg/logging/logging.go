package logging

import (
	"sync"

	"go.uber.org/zap"
)

var (
	logger *zap.Logger
	once   sync.Once
)

// Init initializes the global logger. If called multiple times, only the first takes effect.
func Init(production bool) error {
	var err error
	once.Do(func() {
		if production {
			logger, err = zap.NewProduction()
		} else {
			logger, err = zap.NewDevelopment()
		}
	})
	return err
}

// L returns the global logger, initializing a production logger if needed.
func L() *zap.Logger {
	if logger == nil {
		// ignore error; fall back to no-op logger if creation fails
		_ = Init(true)
		if logger == nil {
			return zap.NewNop()
		}
	}
	return logger
}
