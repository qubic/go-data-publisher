package rpc

import (
	"context"
	"fmt"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/qubic/status-service/domain"
	"github.com/qubic/status-service/protobuf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/emptypb"
	"net"
	"net/http"
)

type StatusProvider interface {
	GetLastProcessedTick() (tick uint32, err error)
	GetSkippedTicks() ([]uint32, error)
	GetCurrentEpoch() (uint32, error)
	GetSourceStatus() (*domain.Status, error)
}

var _ protobuf.StatusServiceServer = &StatusServiceServer{}

type StatusServiceServer struct {
	protobuf.UnimplementedStatusServiceServer
	listenAddrGRPC      string
	listenAddrHTTP      string
	sp                  StatusProvider
	cachedTickIntervals *protobuf.GetTickIntervalsResponse
}

func NewStatusServiceServer(listenAddrGRPC string, listenAddrHTTP string, sp StatusProvider) *StatusServiceServer {
	return &StatusServiceServer{listenAddrGRPC: listenAddrGRPC, listenAddrHTTP: listenAddrHTTP, sp: sp}
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
	epoch, err := s.sp.GetCurrentEpoch()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "retrieving current epoch: %v", err)
	}

	if s.cachedTickIntervals == nil || epoch > s.cachedTickIntervals.CurrentEpoch {
		s.cachedTickIntervals, err = createTickIntervalResponse(s.sp)
		if err != nil {
			return nil, err
		}
	}

	return s.cachedTickIntervals, nil
}

func createTickIntervalResponse(sp StatusProvider) (*protobuf.GetTickIntervalsResponse, error) {
	st, err := sp.GetSourceStatus()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "getting status information: %v", err)
	}

	intervalCount := len(st.TickIntervals)
	var intervals []*protobuf.TickInterval
	for i, interval := range st.TickIntervals {
		if i < intervalCount-1 { // skip last interval
			intervals = append(intervals, &protobuf.TickInterval{
				Epoch:     interval.Epoch,
				FirstTick: interval.From,
				LastTick:  interval.To,
			})
		}
	}
	response := &protobuf.GetTickIntervalsResponse{
		CurrentEpoch:  st.Epoch,
		InitialTick:   st.InitialTick,
		TickIntervals: intervals,
	}
	return response, nil
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
