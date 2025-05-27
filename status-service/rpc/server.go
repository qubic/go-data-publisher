package rpc

import (
	"context"
	"fmt"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/jellydator/ttlcache/v3"
	"github.com/pkg/errors"
	"github.com/qubic/go-data-publisher/status-service/domain"
	"github.com/qubic/go-data-publisher/status-service/protobuf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"net"
	"net/http"
	"sync"
)

type StatusProvider interface {
	GetLastProcessedTick() (tick uint32, err error)
	GetSkippedTicks() ([]uint32, error)
	GetSourceStatus() (*domain.Status, error)
	GetArchiverStatus() (*protobuf.GetArchiverStatusResponse, error)
}

var _ protobuf.StatusServiceServer = &StatusServiceServer{}

const tickIntervalsKey = "tick_intervals"
const archiverStatusKey = "archiver_status"

type StatusServiceServer struct {
	protobuf.UnimplementedStatusServiceServer
	listenAddrGRPC     string
	listenAddrHTTP     string
	sp                 StatusProvider
	messageCache       *ttlcache.Cache[string, proto.Message]
	tickIntervalLock   sync.Mutex
	archiverStatusLock sync.Mutex
}

func NewStatusServiceServer(listenAddrGRPC string, listenAddrHTTP string, sp StatusProvider, messageCache *ttlcache.Cache[string, proto.Message]) *StatusServiceServer {
	return &StatusServiceServer{
		listenAddrGRPC: listenAddrGRPC,
		listenAddrHTTP: listenAddrHTTP,
		sp:             sp,
		messageCache:   messageCache,
	}
}

func (s *StatusServiceServer) GetStatus(context.Context, *emptypb.Empty) (*protobuf.GetStatusResponse, error) {
	lastProcessedTick, err := s.sp.GetLastProcessedTick()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "getting last processed tick: %v", err)
	}

	return &protobuf.GetStatusResponse{LastProcessedTick: lastProcessedTick}, nil
}

func (s *StatusServiceServer) GetHealthCheck(context.Context, *emptypb.Empty) (*protobuf.GetHealthCheckResponse, error) {
	return &protobuf.GetHealthCheckResponse{Status: "UP"}, nil
}

func (s *StatusServiceServer) GetArchiverStatus(context.Context, *emptypb.Empty) (*protobuf.GetArchiverStatusResponse, error) {
	var response *protobuf.GetArchiverStatusResponse
	s.archiverStatusLock.Lock() // lock so that we do not get multiple threads inside the `if`
	item := s.messageCache.Get(archiverStatusKey)
	if item == nil {
		var err error
		response, err = createArchiverStatusResponse(s.sp)
		if err != nil {
			s.archiverStatusLock.Unlock()
			return nil, status.Errorf(codes.Internal, "creating archiver status: %v", err)
		}
		s.messageCache.Set(archiverStatusKey, response, ttlcache.DefaultTTL)
	} else {
		response = item.Value().(*protobuf.GetArchiverStatusResponse)
	}
	s.archiverStatusLock.Unlock()
	return response, nil
}

func (s *StatusServiceServer) GetTickIntervals(context.Context, *emptypb.Empty) (*protobuf.GetTickIntervalsResponse, error) {
	var response *protobuf.GetTickIntervalsResponse
	s.tickIntervalLock.Lock() // lock so that we do not get multiple threads inside the `if`
	item := s.messageCache.Get(tickIntervalsKey)
	if item == nil {
		var err error
		response, err = createTickIntervalResponse(s.sp)
		if err != nil {
			s.tickIntervalLock.Unlock()
			return nil, status.Errorf(codes.Internal, "creating tick interval response: %v", err)
		}
		s.messageCache.Set(tickIntervalsKey, response, ttlcache.DefaultTTL)
	} else {
		response = item.Value().(*protobuf.GetTickIntervalsResponse)
	}
	s.tickIntervalLock.Unlock()
	return response, nil
}

func (s *StatusServiceServer) GetSkippedTicks(context.Context, *emptypb.Empty) (*protobuf.GetSkippedTicksResponse, error) {
	ticks, err := s.sp.GetSkippedTicks()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "calling provider to get skipped ticks: %v", err)
	}

	return &protobuf.GetSkippedTicksResponse{SkippedTicks: ticks}, nil
}

func createArchiverStatusResponse(sp StatusProvider) (*protobuf.GetArchiverStatusResponse, error) {
	lastProcessedTick, err := sp.GetLastProcessedTick()
	if err != nil {
		return nil, errors.Wrap(err, "getting last processed tick")
	}
	archiverStatus, err := sp.GetArchiverStatus()
	if err != nil {
		return nil, errors.Wrap(err, "getting archiver status")
	}
	// replace last processed tick
	archiverStatus.LastProcessedTick.TickNumber = lastProcessedTick
	lastIntervals := archiverStatus.ProcessedTickIntervalsPerEpoch[len(archiverStatus.LastProcessedTicksPerEpoch)-1].Intervals
	lastIntervals[len(lastIntervals)-1].LastProcessedTick = lastProcessedTick
	return archiverStatus, nil
}

func createTickIntervalResponse(sp StatusProvider) (*protobuf.GetTickIntervalsResponse, error) {
	sourceStatus, err := sp.GetSourceStatus()
	if err != nil {
		return nil, errors.Wrap(err, "getting source status")
	}

	lastProcessedTick, err := sp.GetLastProcessedTick()
	if err != nil {
		return nil, errors.Wrap(err, "getting last processed tick")
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
