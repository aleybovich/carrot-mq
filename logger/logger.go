package logger

// Logger interface definition
type Logger interface {
	Fatal(format string, a ...any)
	Err(format string, a ...any)
	Warn(format string, a ...any)
	Info(format string, a ...any)
	Debug(format string, a ...any)
}
