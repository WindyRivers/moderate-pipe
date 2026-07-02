// Command user-service runs the gRPC User Service. It owns the users table and
// answers reputation/profile queries from the Review Service.
package main

import (
	"context"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/WindyRivers/moderate-pipe/internal/config"
	"github.com/WindyRivers/moderate-pipe/internal/store"
	"github.com/WindyRivers/moderate-pipe/pkg/logger"
	userpb "github.com/WindyRivers/moderate-pipe/proto/gen"
	"github.com/WindyRivers/moderate-pipe/services/user"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

func main() {
	cfg, err := config.Load("user-service")
	if err != nil {
		panic(err)
	}
	if err := logger.Init(cfg.App.Name, cfg.App.Env, cfg.App.LogLevel); err != nil {
		panic(err)
	}
	defer logger.Sync()
	log := logger.L()

	db, err := store.NewDB(cfg)
	if err != nil {
		log.Fatal("connect mysql", zap.Error(err))
	}
	if err := store.AutoMigrate(db); err != nil {
		log.Fatal("automigrate", zap.Error(err))
	}

	repo := user.NewRepo(db)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := repo.Seed(ctx); err != nil {
		log.Warn("seed users", zap.Error(err))
	}
	cancel()

	grpcServer := grpc.NewServer()
	userpb.RegisterUserServiceServer(grpcServer, user.NewServer(repo))

	// A standard gRPC health service so compose/k8s can probe readiness and so
	// the Review Service's client-side health checks have something to hit.
	hs := health.NewServer()
	hs.SetServingStatus("user.v1.UserService", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, hs)
	reflection.Register(grpcServer)

	lis, err := net.Listen("tcp", cfg.GRPC.UserServiceListen)
	if err != nil {
		log.Fatal("listen", zap.String("addr", cfg.GRPC.UserServiceListen), zap.Error(err))
	}

	go func() {
		log.Info("user-service listening", zap.String("addr", cfg.GRPC.UserServiceListen))
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatal("grpc serve", zap.Error(err))
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Info("shutting down user-service")
	grpcServer.GracefulStop()
}
