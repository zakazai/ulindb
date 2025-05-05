package types

import (
	"io"
	"log"
	"os"
)

// LogLevel represents the logging level
type LogLevel int

// Log levels
const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarning
	LogLevelError
	LogLevelNone // Disables all logging
)

// Logger provides structured logging for the application
type Logger struct {
	debugLogger   *log.Logger
	infoLogger    *log.Logger
	warningLogger *log.Logger
	errorLogger   *log.Logger
	currentLevel  LogLevel
}

// Global logger instance
var GlobalLogger *Logger

// InitLogger creates a new logger with the specified level
func InitLogger(level LogLevel, output io.Writer) *Logger {
	if output == nil {
		output = os.Stdout
	}

	return &Logger{
		debugLogger:   log.New(output, "DEBUG: ", log.Ldate|log.Ltime),
		infoLogger:    log.New(output, "INFO: ", log.Ldate|log.Ltime),
		warningLogger: log.New(output, "WARNING: ", log.Ldate|log.Ltime),
		errorLogger:   log.New(output, "ERROR: ", log.Ldate|log.Ltime),
		currentLevel:  level,
	}
}

// SetLevel changes the logging level
func (l *Logger) SetLevel(level LogLevel) {
	l.currentLevel = level
}

// GetLevel returns the current logging level
func (l *Logger) GetLevel() LogLevel {
	return l.currentLevel
}

// Debug logs a debug message
func (l *Logger) Debug(format string, v ...interface{}) {
	if l.currentLevel <= LogLevelDebug {
		l.debugLogger.Printf(format, v...)
	}
}

// Info logs an info message
func (l *Logger) Info(format string, v ...interface{}) {
	if l.currentLevel <= LogLevelInfo {
		l.infoLogger.Printf(format, v...)
	}
}

// Warning logs a warning message
func (l *Logger) Warning(format string, v ...interface{}) {
	if l.currentLevel <= LogLevelWarning {
		l.warningLogger.Printf(format, v...)
	}
}

// Error logs an error message
func (l *Logger) Error(format string, v ...interface{}) {
	if l.currentLevel <= LogLevelError {
		l.errorLogger.Printf(format, v...)
	}
}

// Initialize the global logger with default settings
func init() {
	GlobalLogger = InitLogger(LogLevelInfo, os.Stdout)
}