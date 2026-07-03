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

const (
	// migrationLockName is the MySQL advisory lock all services contend for
	// before migrating. It is namespaced so it can't collide with any other
	// GET_LOCK usage on a shared server.
	migrationLockName = "moderate_pipe:automigrate"
	// migrationLockWait is how long GET_LOCK blocks waiting for the holder to
	// finish before giving up. Migration is tiny, so this is generous.
	migrationLockWait = 30 * time.Second
	// migrationTimeout bounds the whole acquire-migrate-release cycle.
	migrationTimeout = 90 * time.Second
)

// AutoMigrate creates or updates the schema for all models. Model definitions
// are the single source of truth; AutoMigrate only ever adds columns/indexes.
//
// All three services share one database and each runs AutoMigrate on boot, so
// on a fresh compose stack they would otherwise race: GORM checks whether a
// table exists and issues CREATE TABLE non-atomically, so two services can both
// decide `posts` is missing and the loser crashes with "Error 1050: table
// already exists". We serialize migration across processes with a MySQL
// advisory lock (GET_LOCK) — the same approach golang-migrate uses: the first
// service in migrates while the others block on the lock, then find the schema
// already present and no-op.
//
// The lock is session-scoped, so it must be acquired and released on a single
// dedicated connection held for the whole migration; releasing early (or the
// connection closing) frees it for the next waiter.
func AutoMigrate(db *gorm.DB) error {
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), migrationTimeout)
	defer cancel()

	// Pin one connection for the lock's whole lifetime — GET_LOCK/RELEASE_LOCK
	// only affect the session that ran them.
	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration connection: %w", err)
	}
	defer conn.Close()

	var acquired sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)",
		migrationLockName, int(migrationLockWait.Seconds())).Scan(&acquired); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	// GET_LOCK returns 1 on success, 0 on timeout, NULL on error.
	if !acquired.Valid || acquired.Int64 != 1 {
		return fmt.Errorf("timed out waiting for migration lock %q after %s",
			migrationLockName, migrationLockWait)
	}
	defer func() {
		// Best-effort release on the same connection; the deferred Close would
		// also free a session lock, this just returns it promptly.
		if _, err := conn.ExecContext(context.Background(),
			"DO RELEASE_LOCK(?)", migrationLockName); err != nil {
			logger.L().Warn("release migration lock", zap.Error(err))
		}
	}()

	if err := db.AutoMigrate(model.AllModels()...); err != nil {
		return err
	}
	logger.L().Info("schema migration complete")
	return nil
}
