// Command review-service consumes moderation tasks and applies the rule engine.
// Run multiple instances with the same KAFKA_CONSUMER_GROUP to load-balance
// consumption across Kafka partitions.
package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/WindyRivers/moderate-pipe/internal/config"
	"github.com/WindyRivers/moderate-pipe/internal/store"
	"github.com/WindyRivers/moderate-pipe/pkg/kafkax"
	"github.com/WindyRivers/moderate-pipe/pkg/logger"
	"github.com/WindyRivers/moderate-pipe/services/review"
	"github.com/WindyRivers/moderate-pipe/services/user"
	"go.uber.org/zap"
)

func main() {
	cfg, err := config.Load("review-service")
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

	repo := review.NewRepo(db)
	userRepo := user.NewRepo(db)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	seedCtx, seedCancel := context.WithTimeout(ctx, 10*time.Second)
	if err := repo.SeedSensitiveWords(seedCtx); err != nil {
		log.Warn("seed sensitive words", zap.Error(err))
	}
	words, err := repo.LoadSensitiveWords(seedCtx)
	seedCancel()
	if err != nil {
		log.Fatal("load sensitive words", zap.Error(err))
	}
	log.Info("loaded sensitive-word block list", zap.Int("count", len(words)))
	engine := review.NewEngine(words)

	// Ensure the DLQ topic exists (single partition is fine for a low-volume
	// dead-letter stream).
	if err := kafkax.EnsureTopic(cfg.Kafka.Brokers, cfg.Kafka.DeadLetterTopic, 1, 1); err != nil {
		log.Fatal("ensure dlq topic", zap.Error(err))
	}

	userClient, err := review.NewUserClient(cfg.GRPC.UserServiceAddr)
	if err != nil {
		log.Fatal("dial user-service", zap.Error(err))
	}
	defer userClient.Close()

	consumer := kafkax.NewConsumer(cfg.Kafka.Brokers, cfg.Kafka.ReviewTopic, cfg.Kafka.ConsumerGroup)
	defer consumer.Close()
	resultProd := kafkax.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.ResultTopic)
	defer resultProd.Close()
	dlqProd := kafkax.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.DeadLetterTopic)
	defer dlqProd.Close()

	instanceID := os.Getenv("INSTANCE_ID")
	if instanceID == "" {
		instanceID, _ = os.Hostname()
	}

	worker := review.NewWorker(consumer, resultProd, dlqProd, repo, userRepo, userClient, engine, instanceID)

	// Minimal health endpoint so compose can report the instance up.
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		})
		srv := &http.Server{Addr: ":8081", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		_ = srv.ListenAndServe()
	}()

	go worker.Run(ctx)
	log.Info("review-service running", zap.String("instance", instanceID),
		zap.String("group", cfg.Kafka.ConsumerGroup))

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Info("shutting down review-service")
	cancel()
	time.Sleep(500 * time.Millisecond) // let the worker finish its current commit
}
