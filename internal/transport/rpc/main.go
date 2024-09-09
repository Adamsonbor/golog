package rpc

import (
	"context"
	v1 "logger/gen/go/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
)

const (
	objectWildcard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

var _ v1.LogServer = (*GRPCServer)(nil)

type Authorizer interface {
	Authorize(subject, object, action string) error
}

type CommitLog interface {
	Append(*v1.Record) (uint64, error)
	Read(uint64) (*v1.Record, error)
}

type SubjectContextKey struct{}

type Config struct {
	CommitLog CommitLog
	Authorize Authorizer
}

type GRPCServer struct {
	*v1.UnimplementedLogServer
	*Config
}

func New(config *Config, opts ...grpc.ServerOption) (*grpc.Server, error) {
	opts = append(opts,
		grpc.StreamInterceptor(
			grpc_middleware.ChainStreamServer(
				grpc_auth.StreamServerInterceptor(authenticate),
			)),
		grpc.UnaryInterceptor(
			grpc_auth.UnaryServerInterceptor(authenticate),
		),
	)
	gsrv := grpc.NewServer(opts...)
	srt, err := new(config)
	if err != nil {
		return nil, err
	}

	v1.RegisterLogServer(gsrv, srt)

	return gsrv, nil
}

func new(config *Config) (srv *GRPCServer, err error) {
	srv = &GRPCServer{
		Config: config,
	}

	return srv, nil
}

func (self *GRPCServer) Produce(ctx context.Context, req *v1.ProduceRequest) (*v1.ProduceResponse, error) {
	err := self.Authorize.Authorize(
		subject(ctx),
		objectWildcard,
		produceAction,
	)
	if err != nil {
		return nil, err
	}

	offset, err := self.Config.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}

	return &v1.ProduceResponse{Offset: offset}, nil
}

func (self *GRPCServer) Consume(ctx context.Context, req *v1.ConsumeRequest) (*v1.ConsumeResponse, error) {
	err := self.Authorize.Authorize(
		subject(ctx),
		objectWildcard,
		consumeAction,
	)
	if err != nil {
		return nil, err
	}
	record, err := self.Config.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}

	return &v1.ConsumeResponse{Record: record}, nil
}

func (self *GRPCServer) ProduceStream(stream v1.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		res, err := self.Produce(stream.Context(), req)
		if err != nil {
			return err
		}

		err = stream.Send(res)
		if err != nil {
			return err
		}
	}
}

func (self *GRPCServer) ConsumeStream(
	req *v1.ConsumeRequest,
	stream v1.Log_ConsumeStreamServer,
) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := self.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case ErrOffsetOutOfRange:
				continue
			default:
				return err
			}

			err = stream.Send(res)
			if err != nil {
				return err
			}

			req.Offset++
		}
	}
}

// Get peer info from context and validate it.
func authenticate(ctx context.Context) (context.Context, error) {
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unknown, "couldn't find peer info")
	}

	if peer.AuthInfo == nil {
		return nil, status.Error(codes.Unauthenticated, "missing credentials")
	}

	tlsInfo := peer.AuthInfo.(credentials.TLSInfo)
	subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, SubjectContextKey{}, subject)

	return ctx, nil
}

func subject(ctx context.Context) string {
	return ctx.Value(SubjectContextKey{}).(string)
}
