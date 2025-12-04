package polardbx

import (
	"log"
	"os"
	"runtime/debug"
	"strings"
)

const (
	LevelSilent = iota
	LevelError
	LevelWarn
	LevelInfo
	LevelDebug
)

var level = LevelWarn

func init() {
	levelStr := os.Getenv("LOG_LEVEL")
	l := ParseLevel(levelStr)
	SetLevel(l)
}

func SetLevel(l int) {
	level = l
}

func IsEnabledFor(l int) bool {
	return level >= l
}

func ParseLevel(levelStr string) int {
	switch strings.ToLower(levelStr) {
	case "debug":
		return LevelDebug
	case "info":
		return LevelInfo
	case "warn", "warning":
		return LevelWarn
	case "error":
		return LevelError
	case "silent":
		return LevelSilent
	default:
		return LevelWarn
	}
}

type Logger struct {
	color string
	log   *log.Logger
}

func (logger *Logger) Info(msg string, enableLog bool) {
	if !enableLog || !IsEnabledFor(LevelInfo) {
		return
	}
	logger.log.Printf("%s[INFO] %s%s\n", logger.color, msg, reset)
}

func (logger *Logger) Error(msg string, enableLog bool) {
	if !enableLog || !IsEnabledFor(LevelError) {
		return
	}
	logger.log.Printf("%s[ERROR] %+v%s\n", logger.color, msg, reset)
}

func (logger *Logger) ErrorWithStack(err error, enableLog bool) {
	if !enableLog || !IsEnabledFor(LevelError) {
		return
	}
	logger.log.Printf("%s[ERROR_STACK] %+v%s\n%s\n", logger.color, err, reset, debug.Stack())
}

func (logger *Logger) Warn(msg string, enableLog bool) {
	if !enableLog || !IsEnabledFor(LevelWarn) {
		return
	}
	logger.log.Printf("%s[WARN] %s%s\n", logger.color, msg, reset)
}

func (logger *Logger) Debug(msg string, enableLog bool) {
	if !enableLog || !IsEnabledFor(LevelDebug) {
		return
	}
	logger.log.Printf("%s[DEBUG] %s%s\n", logger.color, msg, reset)
}

var mainLogger = Logger{color: green, log: log.New(os.Stdout, "[polardbx-connector-go]", log.Ldate|log.Ltime|log.Lmicroseconds)}
var checkLogger = Logger{color: blue, log: log.New(os.Stdout, "[polardbx-connector-go]", log.Ldate|log.Ltime|log.Lmicroseconds)}
