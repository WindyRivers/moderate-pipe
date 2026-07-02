// Command content-service runs the HTTP front door. It accepts posts (writing
// them pending and enqueuing a Kafka moderation task), serves moderation-status
// lookups and the approved-only feed, and consumes review results to flip a
// post's status.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/WindyRivers/moderate-pipe/internal/config"
	"github.com/WindyRivers/moderate-pipe/internal/store"
	"github.com/WindyRivers/moderate-pipe/pkg/kafkax"
	"github.com/WindyRivers/moderate-pipe/pkg/logger"
	"github.com/WindyRivers/moderate-pipe/pkg/ratelimit"
	"github.com/WindyRivers/moderate-pipe/services/content"
	"go.uber.org/zap"
)

func main() {
	cfg, err := config.Load("content-service")
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
	rdb, err := store.NewRedis(cfg)
	if err != nil {
		log.Fatal("connect redis", zap.Error(err))
	}

	// Create the topics we produce to / consume from with the configured
	// partition count (auto-creation would make single-partition topics).
	for _, t := range []string{cfg.Kafka.ReviewTopic, cfg.Kafka.ResultTopic} {
		if err := kafkax.EnsureTopic(cfg.Kafka.Brokers, t, cfg.Kafka.Partitions, 1); err != nil {
			log.Fatal("ensure topic", zap.String("topic", t), zap.Error(err))
		}
	}

	producer := kafkax.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.ReviewTopic)
	defer producer.Close()

	repo := content.NewRepo(db)
	cache := content.NewStatusCache(rdb)
	// 200 posts/sec sustained with a 400 burst: generous enough not to disturb
	// normal use, low enough to shield the pipeline from a runaway client.
	limiter := ratelimit.New(rdb, 200, 400)
	svc := content.NewService(repo, cache, producer, limiter)

	// Result consumer runs in the background flipping statuses.
	resultConsumer := kafkax.NewConsumer(cfg.Kafka.Brokers, cfg.Kafka.ResultTopic, "content-service")
	defer resultConsumer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	go content.NewResultConsumer(resultConsumer, svc).Run(ctx)

	var ready atomic.Bool
	ready.Store(true)
	handler := content.NewHandler(svc, ready.Load)

	srv := &http.Server{
		Addr:              addr(cfg.App.HTTPPort),
		Handler:           handler.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Info("content-service listening", zap.String("addr", srv.Addr))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal("http serve", zap.Error(err))
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Info("shutting down content-service")
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	_ = srv.Shutdown(shutdownCtx)
}

func addr(port int) string {
	return ":" + itoa(port)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		p--
		b[p] = '-'
	}
	return string(b[p:])
}
