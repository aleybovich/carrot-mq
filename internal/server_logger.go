package internal

import (
	"fmt"
	"os"

	"github.com/aleybovich/carrot-mq/logger"
)

func (s *server) Logger() logger.Logger {
	// If using a custom logger, return it
	if s.customLogger != nil {
		return s.customLogger
	}
	// Otherwise return the internal logger (server itself implements Logger interface)
	return s
}

// Fatal logs a message with Fatal level and exits with code 1
func (s *server) Fatal(format string, args ...interface{}) {
	// If using a custom logger, delegate to it
	if s.customLogger != nil && s.customLogger != s {
		s.customLogger.Fatal(format, args...)
		return
	}

	funcName := getCallerName()

	if IsTerminal {
		prefix := fmt.Sprintf("%s[FATAL]%s %s%s%s: ", colorBoldRed, colorReset, colorCyan, funcName, colorReset)
		s.internalLogger.Printf(prefix+format, args...)
	} else {
		s.internalLogger.Printf("[FATAL] %s: "+format, append([]interface{}{funcName}, args...)...)
	}

	os.Exit(1) // Exit with error code 1
}

// Err logs a message with Error level
func (s *server) Err(format string, args ...interface{}) {
	// If using a custom logger, delegate to it
	if s.customLogger != nil && s.customLogger != s {
		s.customLogger.Err(format, args...)
		return
	}

	funcName := getCallerName()

	if IsTerminal {
		prefix := fmt.Sprintf("%s[ERROR]%s %s%s%s: ", colorBoldRed, colorReset, colorCyan, funcName, colorReset)
		s.internalLogger.Printf(prefix+format, args...)
	} else {
		s.internalLogger.Printf("[ERROR] %s: "+format, append([]interface{}{funcName}, args...)...)
	}
}

// Warn logs a message with Warning level
func (s *server) Warn(format string, args ...interface{}) {
	// If using a custom logger, delegate to it
	if s.customLogger != nil && s.customLogger != s {
		s.customLogger.Warn(format, args...)
		return
	}

	funcName := getCallerName()

	if IsTerminal {
		prefix := fmt.Sprintf("%s[WARN]%s %s%s%s: ", colorYellow, colorReset, colorCyan, funcName, colorReset)
		s.internalLogger.Printf(prefix+format, args...)
	} else {
		s.internalLogger.Printf("[WARN] %s: "+format, append([]interface{}{funcName}, args...)...)
	}
}

// Info logs a message with Info level
func (s *server) Info(format string, args ...interface{}) {
	// If using a custom logger, delegate to it
	if s.customLogger != nil && s.customLogger != s {
		s.customLogger.Info(format, args...)
		return
	}

	funcName := getCallerName()

	if IsTerminal {
		prefix := fmt.Sprintf("%s[INFO]%s %s%s%s: ", colorGreen, colorReset, colorCyan, funcName, colorReset)
		s.internalLogger.Printf(prefix+format, args...)
	} else {
		s.internalLogger.Printf("[INFO] %s: "+format, append([]interface{}{funcName}, args...)...)
	}
}

// Debug logs a message with Debug level
func (s *server) Debug(format string, args ...interface{}) {
	// If using a custom logger, delegate to it
	if s.customLogger != nil && s.customLogger != s {
		s.customLogger.Debug(format, args...)
		return
	}

	// Only log debug messages if AMQP_DEBUG environment variable is set
	if os.Getenv("AMQP_DEBUG") != "1" {
		return
	}

	funcName := getCallerName()

	if IsTerminal {
		prefix := fmt.Sprintf("%s[DEBUG]%s %s%s%s: ", colorPurple, colorReset, colorCyan, funcName, colorReset)
		s.internalLogger.Printf(prefix+format, args...)
	} else {
		s.internalLogger.Printf("[DEBUG] %s: "+format, append([]interface{}{funcName}, args...)...)
	}
}
