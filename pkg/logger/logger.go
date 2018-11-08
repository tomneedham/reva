package logger

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
)

// Logger is a simple logger that logs messages to the given io.Writer.
// If a key is set, it will check if it is present in the given context
// and use it as a trace key.
type Logger struct {
	out    io.Writer
	key    interface{}
	module string
}

// New returns a new logger that writes messages to an io.Writer with the prefix
// for the messages given by "module".
func New(out io.Writer, module string, key interface{}) *Logger {
	if out == nil {
		out = ioutil.Discard
	}
	return &Logger{
		out:    out,
		module: module,
		key:    key,
	}
}

// Log logs the message with the given context.
func (l *Logger) Log(ctx context.Context, msg string) {
	trace := l.getTraceFromCtx(ctx)
	fmt.Fprintf(l.out, "%s: trace=%s %s\n", l.module, trace, msg)
}

// Logf logs the message ala fmt.Printf().
func (l *Logger) Logf(ctx context.Context, msg string, params ...interface{}) {
	trace := l.getTraceFromCtx(ctx)
	fmt.Fprintf(l.out, "%s: trace=%s %s\n", l.module, trace, fmt.Sprintf(msg, params...))
}

// Error is a convenience function to log an error.
// Instead of calling l.Log(ctx, err.Error()) is possible to
// use l.Error(ctx, err)
func (l *Logger) Error(ctx context.Context, err error) {
	trace := l.getTraceFromCtx(ctx)
	fmt.Fprintf(l.out, "%s: trace=%s %s\n", l.module, trace, err.Error())
}

func (l *Logger) getTraceFromCtx(ctx context.Context) string {
	trace, _ := ctx.Value(l.key).(string)
	if trace == "" {
		trace = "notrace"
	}
	return trace
}
