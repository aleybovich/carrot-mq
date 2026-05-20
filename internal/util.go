package internal

import (
	"runtime"
	"strings"
)

// Get caller function name for logging
func getCallerName() string {
	pc, _, _, _ := runtime.Caller(2) // Use depth 2 to get the actual caller, not the logging function
	caller := runtime.FuncForPC(pc).Name()
	parts := strings.Split(caller, ".")
	return parts[len(parts)-1]
}
