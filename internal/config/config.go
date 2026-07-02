// Package config centralises the configuration for all three services. Each
// binary loads the same struct from environment variables and only reads the
// sections it needs, which keeps the compose file and .env in one shared
// vocabulary (MYSQL_*, REDIS_*, KAFKA_*, GRPC_*).
package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config is the root configuration shared by the Content, Review and User
// services. Fields map to env vars such as MYSQL_HOST, KAFKA_BROKERS, etc.
type Config struct {
	App   AppConfig
	MySQL MySQLConfig
	Redis RedisConfig
	Kafka KafkaConfig
	GRPC  GRPCConfig
}

type AppConfig struct {
	Name     string // logical service name, set per-binary
	Env      string // development | production
	HTTPPort int    // HTTP port for the Content Service / health endpoints
	LogLevel string
}

type MySQLConfig struct {
	Host            string
	Port            int
	User            string
	Password        string
	Database        string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}

// KafkaConfig holds broker addresses, topic names and the consumer group id.
// Topics are configuration (not hardcoded) so tests can point at throwaway
// topics and so the dead-letter topic name stays consistent across producers
// and consumers.
type KafkaConfig struct {
	Brokers        []string
	ReviewTopic    string // post-review-topic: new posts awaiting moderation
	ResultTopic    string // review-result-topic: moderation outcomes fanned back
	DeadLetterTopic string // post-review-dlq: messages that exhausted retries
	ConsumerGroup  string // review-service consumer group id
	Partitions     int    // partitions to create for the review topic
}

// GRPCConfig holds the User Service listen address (server side) and the
// address the Review Service dials to reach it (client side).
type GRPCConfig struct {
	UserServiceListen string // e.g. ":9090" (User Service binds this)
	UserServiceAddr   string // e.g. "user-service:9090" (Review Service dials this)
}

// DSN returns the MySQL data source name used by GORM.
func (m MySQLConfig) DSN() string {
	return fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		m.User, m.Password, m.Host, m.Port, m.Database,
	)
}

// Load reads configuration from environment variables (optionally seeded from a
// .env file). The serviceName argument labels the process in logs and defaults.
func Load(serviceName string) (*Config, error) {
	v := viper.New()

	v.SetConfigName(".env")
	v.SetConfigType("env")
	v.AddConfigPath(".")
	v.AddConfigPath("..")
	v.AddConfigPath("../..")
	_ = v.ReadInConfig()

	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	setDefaults(v)

	cfg := &Config{
		App: AppConfig{
			Name:     serviceName,
			Env:      v.GetString("APP_ENV"),
			HTTPPort: v.GetInt("APP_HTTP_PORT"),
			LogLevel: v.GetString("APP_LOG_LEVEL"),
		},
		MySQL: MySQLConfig{
			Host:            v.GetString("MYSQL_HOST"),
			Port:            v.GetInt("MYSQL_PORT"),
			User:            v.GetString("MYSQL_USER"),
			Password:        v.GetString("MYSQL_PASSWORD"),
			Database:        v.GetString("MYSQL_DATABASE"),
			MaxOpenConns:    v.GetInt("MYSQL_MAX_OPEN_CONNS"),
			MaxIdleConns:    v.GetInt("MYSQL_MAX_IDLE_CONNS"),
			ConnMaxLifetime: v.GetDuration("MYSQL_CONN_MAX_LIFETIME"),
		},
		Redis: RedisConfig{
			Addr:     v.GetString("REDIS_ADDR"),
			Password: v.GetString("REDIS_PASSWORD"),
			DB:       v.GetInt("REDIS_DB"),
		},
		Kafka: KafkaConfig{
			Brokers:         v.GetStringSlice("KAFKA_BROKERS"),
			ReviewTopic:     v.GetString("KAFKA_REVIEW_TOPIC"),
			ResultTopic:     v.GetString("KAFKA_RESULT_TOPIC"),
			DeadLetterTopic: v.GetString("KAFKA_DLQ_TOPIC"),
			ConsumerGroup:   v.GetString("KAFKA_CONSUMER_GROUP"),
			Partitions:      v.GetInt("KAFKA_PARTITIONS"),
		},
		GRPC: GRPCConfig{
			UserServiceListen: v.GetString("GRPC_USER_LISTEN"),
			UserServiceAddr:   v.GetString("GRPC_USER_ADDR"),
		},
	}
	return cfg, nil
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("APP_ENV", "development")
	v.SetDefault("APP_HTTP_PORT", 8080)
	v.SetDefault("APP_LOG_LEVEL", "info")

	v.SetDefault("MYSQL_HOST", "127.0.0.1")
	v.SetDefault("MYSQL_PORT", 3306)
	v.SetDefault("MYSQL_USER", "moderate")
	v.SetDefault("MYSQL_PASSWORD", "moderate")
	v.SetDefault("MYSQL_DATABASE", "moderate")
	v.SetDefault("MYSQL_MAX_OPEN_CONNS", 50)
	v.SetDefault("MYSQL_MAX_IDLE_CONNS", 10)
	v.SetDefault("MYSQL_CONN_MAX_LIFETIME", time.Hour)

	v.SetDefault("REDIS_ADDR", "127.0.0.1:6379")
	v.SetDefault("REDIS_PASSWORD", "")
	v.SetDefault("REDIS_DB", 0)

	v.SetDefault("KAFKA_BROKERS", []string{"127.0.0.1:9092"})
	v.SetDefault("KAFKA_REVIEW_TOPIC", "post-review-topic")
	v.SetDefault("KAFKA_RESULT_TOPIC", "review-result-topic")
	v.SetDefault("KAFKA_DLQ_TOPIC", "post-review-dlq")
	v.SetDefault("KAFKA_CONSUMER_GROUP", "review-service")
	v.SetDefault("KAFKA_PARTITIONS", 3)

	v.SetDefault("GRPC_USER_LISTEN", ":9090")
	v.SetDefault("GRPC_USER_ADDR", "127.0.0.1:9090")
}

// IsProduction reports whether the process runs in production mode.
func (c *Config) IsProduction() bool {
	return c.App.Env == "production"
}
