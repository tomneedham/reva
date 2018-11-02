package logger

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
)

type Logger struct {
	out    io.Writer
	key    interface{}
	module string
}

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

func (l *Logger) log(ctx context.Context, msg string) {
	trace := l.getTraceFromCtx(ctx)
	fmt.Fprintf(l.out, "%s: trace=%s %s", l.module, trace, msg)
}

func (l *Logger) logf(ctx context.Context, msg string, params ...interface{}) {
	trace := l.getTraceFromCtx(ctx)
	fmt.Fprintf(l.out, "%s: trace=%s %s", l.module, trace, fmt.Sprintf(msg, params...))
}

func (l *Logger) getTraceFromCtx(ctx context.Context) string {
	trace, _ := ctx.Value(l.key).(string)
	if trace == "" {
		trace = "notrace"
	}
	return trace
}
