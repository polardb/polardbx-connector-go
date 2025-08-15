package polardbx

import (
	"log"
	"os"
)

type Logger struct {
	color string
	log   *log.Logger
}

func (logger *Logger) Info(msg string, enableLog bool) {
	if !enableLog {
		return
	}
	logger.log.Printf("%s[INFO] %s%s\n", logger.color, msg, reset)
}

func (logger *Logger) Error(msg string, enableLog bool) {
	if !enableLog {
		return
	}
	logger.log.Printf("%s[ERROR] %s%s\n", logger.color, msg, reset)
}

func (logger *Logger) Warn(msg string, enableLog bool) {
	if !enableLog {
		return
	}
	logger.log.Printf("%s[WARN] %s%s\n", logger.color, msg, reset)
}

func (logger *Logger) Debug(msg string, enableLog bool) {
	if !enableLog {
		return
	}
	logger.log.Printf("%s[DEBUG] %s%s\n", logger.color, msg, reset)
}

var mainLogger = Logger{color: green, log: log.New(os.Stdout, "[polardbx-connector-go]", log.Ldate|log.Ltime|log.Lmicroseconds)}
var checkLogger = Logger{color: blue, log: log.New(os.Stdout, "[polardbx-connector-go]", log.Ldate|log.Ltime|log.Lmicroseconds)}
