// Package logger wraps uber-go/zap to provide a single, structured logger for
// every service in the pipeline. Structured JSON logs are what make the
// "end-to-end latency" and "chaos drill" observations in this project
// machine-greppable rather than free-text prose.
package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var global *zap.Logger

// Init builds the global logger. In production it emits JSON at the configured
// level; in development it uses a human-friendly console encoder. The service
// name is attached to every line so logs from the three services interleave
// legibly when tailing `docker compose logs`.
func Init(service, env, level string) error {
	lvl := zap.NewAtomicLevel()
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl.SetLevel(zapcore.InfoLevel)
	}

	var cfg zap.Config
	if env == "production" {
		cfg = zap.NewProductionConfig()
	} else {
		cfg = zap.NewDevelopmentConfig()
		cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	}
	cfg.Level = lvl
	cfg.EncoderConfig.TimeKey = "ts"
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	l, err := cfg.Build()
	if err != nil {
		return err
	}
	global = l.With(zap.String("service", service))
	return nil
}

// L returns the global logger, or a no-op logger if Init has not run yet.
func L() *zap.Logger {
	if global == nil {
		return zap.NewNop()
	}
	return global
}

// Sync flushes any buffered log entries. Call it on shutdown.
func Sync() {
	if global != nil {
		_ = global.Sync()
	}
}
