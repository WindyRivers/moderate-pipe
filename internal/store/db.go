// Package store wires up the shared MySQL and Redis connections used across the
// services. The MySQL opener carries over ContentHub's boot-race fix: on a
// fresh compose stack the DB reports healthy (it answers on its local socket) a
// moment before it accepts TCP, so the first dial is often refused; retrying
// for a short window turns that into a non-event instead of a crash loop.
package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/WindyRivers/moderate-pipe/internal/config"
	"github.com/WindyRivers/moderate-pipe/internal/model"
	"github.com/WindyRivers/moderate-pipe/pkg/logger"
	"go.uber.org/zap"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

const (
	dbConnectTimeout = 30 * time.Second
	dbConnectBackoff = 2 * time.Second
	dbPingTimeout    = 3 * time.Second
)

// NewDB opens the MySQL connection pool, retrying until the server accepts
// connections or dbConnectTimeout elapses.
func NewDB(cfg *config.Config) (*gorm.DB, error) {
	logLevel := gormlogger.Warn
	if !cfg.IsProduction() {
		logLevel = gormlogger.Info
	}
	gormCfg := &gorm.Config{
		Logger:                                   gormlogger.Default.LogMode(logLevel),
		PrepareStmt:                              true,
		DisableForeignKeyConstraintWhenMigrating: true,
	}

	db, err := openWithRetry(cfg, gormCfg)
	if err != nil {
		return nil, err
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	sqlDB.SetMaxOpenConns(cfg.MySQL.MaxOpenConns)
	sqlDB.SetMaxIdleConns(cfg.MySQL.MaxIdleConns)
	lifetime := cfg.MySQL.ConnMaxLifetime
	if lifetime == 0 {
		lifetime = time.Hour
	}
	sqlDB.SetConnMaxLifetime(lifetime)
	return db, nil
}

func openWithRetry(cfg *config.Config, gormCfg *gorm.Config) (*gorm.DB, error) {
	deadline := time.Now().Add(dbConnectTimeout)
	for attempt := 1; ; attempt++ {
		db, err := gorm.Open(mysql.Open(cfg.MySQL.DSN()), gormCfg)
		if err == nil {
			var sqlDB *sql.DB
			if sqlDB, err = db.DB(); err == nil {
				ctx, cancel := context.WithTimeout(context.Background(), dbPingTimeout)
				err = sqlDB.PingContext(ctx)
				cancel()
				if err == nil {
					return db, nil
				}
				sqlDB.Close()
			}
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("mysql unreachable after %s: %w", dbConnectTimeout, err)
		}
		logger.L().Warn("mysql not ready, retrying", zap.Int("attempt", attempt), zap.Error(err))
		time.Sleep(dbConnectBackoff)
	}
}

// AutoMigrate creates or updates the schema for all models. Model definitions
// are the single source of truth; AutoMigrate only ever adds columns/indexes.
func AutoMigrate(db *gorm.DB) error {
	return db.AutoMigrate(model.AllModels()...)
}
