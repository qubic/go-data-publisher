package rpc

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/qubic/go-data-publisher/status-service/protobuf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/emptypb"
)

var _ protobuf.StatusServiceServer = &StatusServiceServer{}

const tickIntervalsKey = "tick_intervals"
const archiverStatusKey = "archiver_status"

type StatusServiceServer struct {
	protobuf.UnimplementedStatusServiceServer
	listenAddrGRPC string
	listenAddrHTTP string
	statusCache    *StatusService
}

func NewStatusServiceServer(listenAddrGRPC string, listenAddrHTTP string, statusCache *StatusService) *StatusServiceServer {
	return &StatusServiceServer{
		listenAddrGRPC: listenAddrGRPC,
		listenAddrHTTP: listenAddrHTTP,
		statusCache:    statusCache,
	}
}

func (s *StatusServiceServer) GetStatus(context.Context, *emptypb.Empty) (*protobuf.GetStatusResponse, error) {
	lastProcessedTick, err := s.statusCache.GetLastProcessedTick()
	if err != nil {
		log.Printf("[ERROR] getting status (last processed tick): %v", err)
		return nil, status.Error(codes.Internal, "getting status")
	}
	lastProcessedEpoch, err := s.statusCache.GetLastProcessedEpoch()
	if err != nil {
		log.Printf("[ERROR] getting status (last processed epoch): %v", err)
		return nil, status.Error(codes.Internal, "getting status")
	}

	initialTickOfCurrentTickRange, err := s.statusCache.GetInitialTickOfCurrentTickRange()
	if err != nil {
		log.Printf("[ERROR] getting status (initial tick of current tick range): %v", err)
		return nil, status.Errorf(codes.Internal, "getting status")
	}

	return &protobuf.GetStatusResponse{
		LastProcessedTick:    lastProcessedTick,
		LastProcessedEpoch:   lastProcessedEpoch,
		TickRangeInitialTick: initialTickOfCurrentTickRange,
	}, nil
}

func (s *StatusServiceServer) GetArchiverStatus(context.Context, *emptypb.Empty) (*protobuf.GetArchiverStatusResponse, error) {
	response, err := s.statusCache.GetArchiverStatusResponse()
	if err != nil {
		log.Printf("[ERROR] getting archiver status: %v", err)
		return nil, status.Error(codes.Internal, "getting archiver status")
	}
	return response, nil
}

func (s *StatusServiceServer) GetTickIntervals(ctx context.Context, _ *emptypb.Empty) (*protobuf.GetTickIntervalsResponse, error) {
	response, err := s.statusCache.GetTickIntervals(ctx)
	if err != nil {
		log.Printf("[ERROR] getting tick intervals: %v", err)
		return nil, status.Error(codes.Internal, "getting tick intervals")
	}
	return response, nil
}

func (s *StatusServiceServer) GetErroneousSkippedTicks(context.Context, *emptypb.Empty) (*protobuf.GetSkippedTicksResponse, error) {
	ticks, err := s.statusCache.GetErroneousSkippedTicks()
	if err != nil {
		log.Printf("[ERROR] getting skipped ticks: %v", err)
		return nil, status.Error(codes.Internal, "getting skipped ticks")
	}
	return &protobuf.GetSkippedTicksResponse{SkippedTicks: ticks}, nil
}

func (s *StatusServiceServer) GetHealthCheck(context.Context, *emptypb.Empty) (*protobuf.GetHealthCheckResponse, error) {
	return &protobuf.GetHealthCheckResponse{Status: "UP"}, nil
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
