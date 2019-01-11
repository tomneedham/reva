package log

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"io"
	golog "log"
	"os"
)

var nop = &nopLogger{}
var prefixes = []string{} 
var enabledLoggers map[string]logger = map[string]logger{}
var Out io.Writer = os.Stderr
var OutPath string = "stderr"

type Logger struct {
	prefix string
	pid    int
}

type logger interface {
	Println(ctx context.Context, args ...interface{})
	Printf(ctx context.Context, format string, v ...interface{})
	Error(ctx context.Context, err error)
	Panic(ctx context.Context, reason string)
}

func ListRegisteredPackages() []string {
	return prefixes
}

func ListEnabledPackages() []string {
	pkgs := []string{}
	for k, _ := range enabledLoggers {
		pkgs = append(pkgs, k)
	}
	return pkgs
}

func EnableAll() error {
	for _, v := range prefixes {
		if err := Enable(v); err != nil {
			return err
		}
	}
	return nil
}
func Enable(prefix string) error {
	l, err := create(prefix)
	if err != nil {
		return err
        }
	enabledLoggers[prefix] = l
	return nil
}

func Disable(prefix string) {
	enabledLoggers[prefix] = nop
}

func create(prefix string) (*internalLogger, error) {
	// add whitespace to stdlogger prefix so it is not appended to the date
	stdLogger := golog.New(Out, prefix+" ", golog.LstdFlags|golog.LUTC)
	zapConfig := zap.NewProductionConfig()
	zapConfig.OutputPaths = []string{OutPath}
	zapConfig.Encoding = "console"
	zapConfig.EncoderConfig = zap.NewProductionEncoderConfig()
	zapConfig.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	zapLogger, err := zapConfig.Build(zap.AddCallerSkip(2))
	if err != nil {
		return nil, err
	}
	zapLogger = zapLogger.Named(prefix)
	internalLogger := &internalLogger{prefix: prefix, stdLogger: stdLogger, pid: os.Getpid(), zapLogger: zapLogger}
	return internalLogger, nil
}

func New(prefix string) *Logger {
	prefixes  = append(prefixes, prefix)
	enabledLoggers[prefix] = nop
	logger := &Logger{prefix: prefix}
	return logger
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
	pid       int
	prefix    string
	stdLogger *golog.Logger
	zapLogger *zap.Logger
}

func (l *internalLogger) Println(ctx context.Context, args ...interface{}) {
	msg := fmt.Sprint(args...)
	l.zapLogger.Info(msg, zap.String("trace", l.extractTrace(ctx)), zap.Int("pid", l.pid))
}

func (l *internalLogger) Printf(ctx context.Context, format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	l.zapLogger.Info(msg, zap.String("trace", l.extractTrace(ctx)), zap.Int("pid", l.pid))
}

func (l *internalLogger) Error(ctx context.Context, err error) {
	msg := err.Error()
	l.zapLogger.Info(msg, zap.String("trace", l.extractTrace(ctx)), zap.Int("pid", l.pid))
}

func (l *internalLogger) Panic(ctx context.Context, reason string) {
	l.zapLogger.Error(reason, zap.String("trace", l.extractTrace(ctx)), zap.Int("pid", l.pid))
}

func (l *internalLogger) extractTrace(ctx context.Context) string {
	if v, ok := ctx.Value("trace").(string); ok {
		return v
	}
	return "00000000-0000-0000-0000-000000000000"
}

type nopLogger struct{}

func (l *nopLogger) Println(ctx context.Context, args ...interface{})            {}
func (l *nopLogger) Printf(ctx context.Context, format string, v ...interface{}) {}
func (l *nopLogger) Error(ctx context.Context, err error)                        {}
func (l *nopLogger) Panic(ctx context.Context, reason string)                    {}
