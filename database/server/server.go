package server

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gocql/gocql"
	vyletdatabase "github.com/vylet-app/go/database/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"
)

const (
	grpcTimeout = 10 * time.Minute
)

type Server struct {
	vyletdatabase.UnimplementedProfileServiceServer
	vyletdatabase.UnimplementedPostServiceServer
	vyletdatabase.UnimplementedLikeServiceServer
	vyletdatabase.UnimplementedBlobRefServiceServer

	logger *slog.Logger

	listenerAddr string
	grpcServer   *grpc.Server

	cqlSession *gocql.Session

	cassandraAddrs    []string
	cassandraKeyspace string
}

type Args struct {
	Logger *slog.Logger

	ListenAddr string

	CassandraAddrs    []string
	CassandraKeyspace string
}

func New(args *Args) (*Server, error) {
	if args.Logger == nil {
		args.Logger = slog.Default()
	}

	logger := args.Logger

	certificate, err := GenerateTLSCertificate("localhost")
	if err != nil {
		return nil, fmt.Errorf("failed to generate TLS certificate: %w", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{*certificate},
		MinVersion:   tls.VersionTLS13,
	}
	creds := credentials.NewTLS(tlsConfig)

	grpcServer := grpc.NewServer(
		grpc.Creds(creds),
		grpc.MaxConcurrentStreams(100_000),
		grpc.ConnectionTimeout(grpcTimeout),
	)

	cluster := gocql.NewCluster(args.CassandraAddrs...)
	cluster.Keyspace = args.CassandraKeyspace
	cluster.Consistency = gocql.Quorum
	cluster.ProtoVersion = 4
	cluster.ConnectTimeout = time.Second * 10
	cluster.Timeout = time.Second * 10

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to cassandra: %w", err)
	}

	server := Server{
		logger: logger,

		cassandraAddrs:    args.CassandraAddrs,
		cassandraKeyspace: args.CassandraKeyspace,

		listenerAddr: args.ListenAddr,

		cqlSession: session,

		grpcServer: grpcServer,
	}

	server.registerServices()

	return &server, nil
}

func (s *Server) Run(ctx context.Context) error {
	logger := s.logger.With("name", "Run")

	logger.Info("attempting to listen", "addr", s.listenerAddr)

	listener, err := net.Listen("tcp", s.listenerAddr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	logger.Info("running gRPC server", "addr", s.listenerAddr)

	grpcServerErr := make(chan error, 1)
	go func() {
		logger.Info("starting gRPC server")

		if err := s.grpcServer.Serve(listener); err != nil {
			if err != http.ErrServerClosed {
				logger.Error("gRPC server shutdown with error", "err", err)
				grpcServerErr <- err
				return
			}
			logger.Info("gRPC server shutdown")
			grpcServerErr <- nil
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-signals:
		logger.Info("received exit signal", "signal", sig)
	case <-ctx.Done():
		logger.Info("context cancelled")
	case err := <-grpcServerErr:
		logger.Error("received grpc server error", "err", err)
	}

	s.grpcServer.GracefulStop()
	s.cqlSession.Close()

	logger.Info("gRPC server shut down")

	return nil
}

func (s *Server) registerServices() {
	vyletdatabase.RegisterProfileServiceServer(s.grpcServer, s)
	vyletdatabase.RegisterPostServiceServer(s.grpcServer, s)
	vyletdatabase.RegisterLikeServiceServer(s.grpcServer, s)
	vyletdatabase.RegisterBlobRefServiceServer(s.grpcServer, s)
	reflection.Register(s.grpcServer)
}

func GenerateTLSCertificate(commonName string) (*tls.Certificate, error) {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixMilli()),
		Subject: pkix.Name{
			CommonName: commonName,
		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(10 * 365 * 24 * time.Hour),
	}
	certificate, err := x509.CreateCertificate(rand.Reader, &template, &template, privateKey.Public(), privateKey)
	if err != nil {
		return nil, err
	}
	return &tls.Certificate{
		Certificate: [][]byte{certificate},
		PrivateKey:  privateKey,
	}, nil
}
