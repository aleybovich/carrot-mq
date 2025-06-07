package internal

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

// MockLogger implements the Logger interface for testing
type MockLogger struct {
	logs     map[string][]string // key is log level, value is array of log entries
	mu       sync.Mutex
	t        *testing.T
	logCount int // total log entries count
}

// NewMockLogger creates a new MockLogger for testing
func NewMockLogger(t *testing.T) *MockLogger {
	return &MockLogger{
		logs: map[string][]string{
			"fatal": {},
			"error": {},
			"warn":  {},
			"info":  {},
			"debug": {},
		},
		t: t,
	}
}

func (m *MockLogger) Fatal(format string, a ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	msg := fmt.Sprintf(format, a...)
	m.logs["fatal"] = append(m.logs["fatal"], msg)
	m.logCount++
	m.t.Logf("MOCK-FATAL: %s", msg)
}

func (m *MockLogger) Err(format string, a ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	msg := fmt.Sprintf(format, a...)
	m.logs["error"] = append(m.logs["error"], msg)
	m.logCount++
	m.t.Logf("MOCK-ERROR: %s", msg)
}

func (m *MockLogger) Warn(format string, a ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	msg := fmt.Sprintf(format, a...)
	m.logs["warn"] = append(m.logs["warn"], msg)
	m.logCount++
	m.t.Logf("MOCK-WARN: %s", msg)
}

func (m *MockLogger) Info(format string, a ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	msg := fmt.Sprintf(format, a...)
	m.logs["info"] = append(m.logs["info"], msg)
	m.logCount++
	m.t.Logf("MOCK-INFO: %s", msg)
}

func (m *MockLogger) Debug(format string, a ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	msg := fmt.Sprintf(format, a...)
	m.logs["debug"] = append(m.logs["debug"], msg)
	m.logCount++
	m.t.Logf("MOCK-DEBUG: %s", msg)
}

// Contains checks if any log message at the specified level contains the given substr
func (m *MockLogger) Contains(level, substr string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	logs, ok := m.logs[level]
	if !ok {
		return false
	}

	for _, msg := range logs {
		if strings.Contains(msg, substr) {
			return true
		}
	}
	return false
}

// Count returns the number of log messages at the specified level
func (m *MockLogger) Count(level string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.logs[level])
}

// TotalCount returns the total number of log messages
func (m *MockLogger) TotalCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.logCount
}

func TestCustomLogger(t *testing.T) {
	// Create a mock logger
	mockLogger := NewMockLogger(t)

	// Create a s with the custom logger
	s := NewServer(WithLogger(mockLogger))

	// Verify that the server uses our custom logger
	if s.customLogger != mockLogger {
		t.Errorf("Server did not set custom logger correctly, got %T, want %T", s.customLogger, mockLogger)
	}

	// Test that basic logging works
	t.Run("Test basic logging", func(t *testing.T) {
		// Clear previous log counts
		prevCount := mockLogger.TotalCount()

		// Call each log method
		s.Info("Test info message: %d", 123)
		s.Warn("Test warn message: %s", "warning")
		s.Err("Test error message: %v", fmt.Errorf("test error"))
		s.Debug("Test debug message")

		// Check if logs were recorded correctly
		newCount := mockLogger.TotalCount() - prevCount
		if newCount != 4 {
			t.Errorf("Expected 4 log messages, got %d", newCount)
		}

		// Check info log
		infoCount := mockLogger.Count("info")
		if infoCount < 1 {
			t.Errorf("No info logs recorded")
		}

		// Check warning log
		warnCount := mockLogger.Count("warn")
		if warnCount < 1 {
			t.Errorf("No warning logs recorded")
		}

		// Check error log
		errCount := mockLogger.Count("error")
		if errCount < 1 {
			t.Errorf("No error logs recorded")
		}

		// Debug logs depend on AMQP_DEBUG env var, so we don't assert on them
	})

	// Test that Fatal logs work, but don't actually call os.Exit
	t.Run("Test Fatal without exiting", func(t *testing.T) {
		// Create a custom server wrapper that overrides Fatal to prevent exit
		type TestServer struct {
			*server
		}

		// Override Fatal to not call os.Exit
		testServer := &TestServer{server: NewServer(WithLogger(mockLogger))}

		// The real Fatal method would call os.Exit(1), but our mock logger just records it
		testServer.Fatal("Test fatal message: %s", "critical error")

		// Check if fatal log was recorded
		fatalCount := mockLogger.Count("fatal")
		if fatalCount < 1 {
			t.Errorf("No fatal logs recorded")
		}
	})
}
