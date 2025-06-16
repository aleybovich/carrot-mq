package logger

import "fmt"

// Logger interface definition
type Logger interface {
	Fatal(format string, a ...any)
	Err(format string, a ...any)
	Warn(format string, a ...any)
	Info(format string, a ...any)
	Debug(format string, a ...any)
}

// NilLogger is a logger implementation that doesn't write any logs
type NilLogger struct{}

// Fatal does nothing
func (n *NilLogger) Fatal(format string, a ...any) { panic(fmt.Sprintf(format, a...)) }

// Err does nothing
func (n *NilLogger) Err(format string, a ...any) {}

// Warn does nothing
func (n *NilLogger) Warn(format string, a ...any) {}

// Info does nothing
func (n *NilLogger) Info(format string, a ...any) {}

// Debug does nothing
func (n *NilLogger) Debug(format string, a ...any) {}
