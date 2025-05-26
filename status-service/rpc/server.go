package rpc

import (
	"context"
	"fmt"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/qubic/go-data-publisher/status-service/domain"
	"github.com/qubic/go-data-publisher/status-service/protobuf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/emptypb"
	"net"
	"net/http"
	"sync"
	"time"
)

type StatusProvider interface {
	GetLastProcessedTick() (tick uint32, err error)
	GetSkippedTicks() ([]uint32, error)
	GetSourceStatus() (*domain.Status, error)
}

var _ protobuf.StatusServiceServer = &StatusServiceServer{}

type StatusServiceServer struct {
	protobuf.UnimplementedStatusServiceServer
	listenAddrGRPC      string
	listenAddrHTTP      string
	sp                  StatusProvider
	cachedTickIntervals *protobuf.GetTickIntervalsResponse
	cacheUpdated        time.Time
	mu                  sync.Mutex
}

func NewStatusServiceServer(listenAddrGRPC string, listenAddrHTTP string, sp StatusProvider) *StatusServiceServer {
	return &StatusServiceServer{
		listenAddrGRPC: listenAddrGRPC,
		listenAddrHTTP: listenAddrHTTP,
		sp:             sp,
		cacheUpdated:   time.Now(),
	}
}

func (s *StatusServiceServer) GetSkippedTicks(context.Context, *emptypb.Empty) (*protobuf.GetSkippedTicksResponse, error) {
	ticks, err := s.sp.GetSkippedTicks()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "calling provider to get skipped ticks: %v", err)
	}

	return &protobuf.GetSkippedTicksResponse{SkippedTicks: ticks}, nil
}

func (s *StatusServiceServer) GetStatus(context.Context, *emptypb.Empty) (*protobuf.GetStatusResponse, error) {
	lastProcessedTick, err := s.sp.GetLastProcessedTick()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "calling provider to get last processed tick: %v", err)
	}

	return &protobuf.GetStatusResponse{LastProcessedTick: lastProcessedTick}, nil
}

func (s *StatusServiceServer) GetHealthCheck(context.Context, *emptypb.Empty) (*protobuf.GetHealthCheckResponse, error) {
	return &protobuf.GetHealthCheckResponse{Status: "UP"}, nil
}

func (s *StatusServiceServer) GetTickIntervals(context.Context, *emptypb.Empty) (*protobuf.GetTickIntervalsResponse, error) {
	// cache because this method is called very often and does some computations
	s.mu.Lock() // lock so that we do not get multiple threads inside the `if`
	if s.cachedTickIntervals == nil || s.cacheUpdated.Before(time.Now().Add(-1*time.Second)) {
		// refresh cache
		response, err := createTickIntervalResponse(s.sp)
		if err != nil {
			s.mu.Unlock()
			return nil, err
		}
		s.cachedTickIntervals = response
		s.cacheUpdated = time.Now()
	}
	s.mu.Unlock()

	// default case
	return s.cachedTickIntervals, nil
}

func createTickIntervalResponse(sp StatusProvider) (*protobuf.GetTickIntervalsResponse, error) {
	sourceStatus, err := sp.GetSourceStatus()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "getting status information: %v", err)
	}

	lastProcessedTick, err := sp.GetLastProcessedTick()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "getting last processed tick: %v", err)
	}

	// we don't show the last interval as the to tick might not be up to date
	// we take the initial tick from the last interval as there might be multiple intervals in the last epoch
	intervalsCount := len(sourceStatus.TickIntervals)
	intervals := make([]*protobuf.TickInterval, 0, intervalsCount)
	for i, interval := range sourceStatus.TickIntervals {
		if i < intervalsCount-1 {
			intervals = append(intervals, &protobuf.TickInterval{
				Epoch:     interval.Epoch,
				FirstTick: interval.From,
				LastTick:  interval.To,
			})
		} else { // last interval
			intervals = append(intervals, &protobuf.TickInterval{
				Epoch:     interval.Epoch,
				FirstTick: interval.From,
				LastTick:  lastProcessedTick, // fix last interval
			})
		}
	}
	return &protobuf.GetTickIntervalsResponse{
		Intervals: intervals,
	}, nil
}

func (s *StatusServiceServer) Start(errChan chan error) error {

	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(600*1024*1024),
		grpc.MaxSendMsgSize(600*1024*1024),
	)
	protobuf.RegisterStatusServiceServer(srv, s)
	reflection.Register(srv)

	lis, err := net.Listen("tcp", s.listenAddrGRPC)
	if err != nil {
		return fmt.Errorf("failed to listen on grpc port: %v", err)
	}

	go func() {
		if err := srv.Serve(lis); err != nil {
			errChan <- fmt.Errorf("serving grpc listener: %v", err)
		}
	}()

	if s.listenAddrHTTP == "" {
		return nil
	}

	mux := runtime.NewServeMux(runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{
		MarshalOptions: protojson.MarshalOptions{EmitDefaultValues: true, EmitUnpopulated: true},
	}))
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(600*1024*1024),
			grpc.MaxCallSendMsgSize(600*1024*1024),
		),
	}

	err = protobuf.RegisterStatusServiceHandlerFromEndpoint(context.Background(), mux, s.listenAddrGRPC, opts)
	if err != nil {
		return fmt.Errorf("registering grpc gateway handler: %v", err)
	}

	go func() {
		if err := http.ListenAndServe(s.listenAddrHTTP, mux); err != nil {
			errChan <- fmt.Errorf("serving http listener: %v", err)
		}
	}()

	return nil
}
