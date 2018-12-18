package log

import (
	"context"
	"fmt"
	"io"
	golog "log"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
)

var nop = &nopLogger{}
var internalLoggers map[string]logger = map[string]logger{}
var enabledLoggers map[string]logger = map[string]logger{}
var Out io.Writer = os.Stderr

type Logger struct {
	prefix string
}

type logger interface {
	Println(ctx context.Context, args ...interface{})
	Printf(ctx context.Context, format string, v ...interface{})
	Error(ctx context.Context, err error)
	Panic(ctx context.Context, reason string)
}

func ListRegisteredPackages() []string {
	pkgs := []string{}
	for k, _ := range internalLoggers {
		pkgs = append(pkgs, k)
	}
	return pkgs
}

func ListEnabledPackages() []string {
	pkgs := []string{}
	for k, _ := range enabledLoggers {
		pkgs = append(pkgs, k)
	}
	return pkgs
}

func Enable(prefix string) {
	enabledLoggers[prefix] = internalLoggers[prefix]
}

func Disable(prefix string) {
	enabledLoggers[prefix] = nop
}

func New(prefix string) *Logger {
	// add whitespace to stdlogger prefix so it is not appended to the date
	stdLogger := golog.New(Out, prefix+" ", golog.LstdFlags|golog.LUTC)
	internalLogger := &internalLogger{prefix: prefix, stdLogger: stdLogger}
	internalLoggers[prefix] = internalLogger
	enabledLoggers[prefix] = internalLogger
	logger := &Logger{prefix: prefix}
	return logger
}

func findInternalLogger(prefix string) logger {
	return internalLoggers[prefix]
}
func findEnabledLogger(prefix string) logger {
	return enabledLoggers[prefix]
}

func (l *Logger) Println(ctx context.Context, args ...interface{}) {
	internalLogger := findEnabledLogger(l.prefix)
	internalLogger.Println(ctx, args...)
}

func (l *Logger) Printf(ctx context.Context, format string, args ...interface{}) {
	internalLogger := findEnabledLogger(l.prefix)
	internalLogger.Printf(ctx, format, args...)
}

func (l *Logger) Error(ctx context.Context, err error) {
	internalLogger := findEnabledLogger(l.prefix)
	internalLogger.Error(ctx, err)
}

func (l *Logger) Panic(ctx context.Context, reason string) {
	internalLogger := findEnabledLogger(l.prefix)
	internalLogger.Panic(ctx, reason)
}

type internalLogger struct {
	prefix    string
	stdLogger *golog.Logger
}

func (l *internalLogger) getCaller() (string, int) {
	_, file, line, ok := runtime.Caller(3)
	if !ok {
		return "???", 0
	}

	for i := len(file) - 1; i > 0; i-- {
		if file[i] == '/' {
			return file[i+1:], line
		}
	}

	return file, line
}

func (l *internalLogger) Println(ctx context.Context, args ...interface{}) {
	file, line := l.getCaller()
	trace := l.extractTrace(ctx)
	msg := fmt.Sprintf("%s:%d [%s] [info] %s", file, line, trace, fmt.Sprintln(args...))
	l.stdLogger.Println(msg)
}

func (l *internalLogger) Printf(ctx context.Context, format string, v ...interface{}) {
	file, line := l.getCaller()
	trace := l.extractTrace(ctx)
	msg := fmt.Sprintf("%s:%d [%s] [info] %s", file, line, trace, fmt.Sprintf(format, v...))
	l.stdLogger.Println(msg)
}

func (l *internalLogger) Error(ctx context.Context, err error) {
	file, line := l.getCaller()
	trace := l.extractTrace(ctx)
	msg := fmt.Sprintf("%s:%d [%s] [error] %s", file, line, trace, err)
	l.stdLogger.Println(msg)
}

func (l *internalLogger) Panic(ctx context.Context, reason string) {
	file, line := l.getCaller()
	trace := l.extractTrace(ctx)
	stack := debug.Stack()
	stackString := strings.Replace(string(stack), "\n", "\\n", -1)
	msg := fmt.Sprintf("%s:%d [%s] [panic] %s: %s", file, line, trace, reason, stackString)
	l.stdLogger.Println(msg)
}

func (l *internalLogger) extractTrace(ctx context.Context) string {
	if v, ok := ctx.Value("trace").(string); ok {
		return v
	}
	return "trace-missing"
}

type nopLogger struct{}

func (l *nopLogger) Println(ctx context.Context, args ...interface{})            {}
func (l *nopLogger) Printf(ctx context.Context, format string, v ...interface{}) {}
func (l *nopLogger) Error(ctx context.Context, err error)                        {}
func (l *nopLogger) Panic(ctx context.Context, reason string)                    {}
