package interceptors

import (
	"context"
	"github.com/cernbox/reva/pkg/log"
	"github.com/gofrs/uuid"
	"google.golang.org/grpc"
)

var logger = log.New("grpc-interceptor")

func LogUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		logger.Println(ctx, info.FullMethod, req)
		uuid := uuid.Must(uuid.NewV4()).String()
		ctx = context.WithValue(ctx, "trace", uuid)
		return handler(ctx, req)
	}
}

func TraceUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		uuid := uuid.Must(uuid.NewV4()).String()
		ctx = context.WithValue(ctx, "trace", uuid)
		return handler(ctx, req)
	}
}

func TraceStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		uuid := uuid.Must(uuid.NewV4()).String()
		ctx := context.WithValue(ss.Context(), "trace", uuid)
		wrapped := newWrappedServerStream(ss, ctx)
		return handler(srv, wrapped)
	}
}

func newWrappedServerStream(ss grpc.ServerStream, ctx context.Context) *wrappedServerStream {
	return &wrappedServerStream{ServerStream: ss, newCtx: ctx}
}

type wrappedServerStream struct {
	grpc.ServerStream
	newCtx context.Context
}

func (ss *wrappedServerStream) Context() context.Context {
	return ss.newCtx
}
