package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"landing-connector/internal/app"
	"landing-connector/internal/config"
)

func main() {
	configPath := flag.String("config", "", "Path to the pipeline config file")
	flag.Parse()

	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "missing required -config flag")
		os.Exit(2)
	}

	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "build logger: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = logger.Sync() }()

	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Fatal("load config", zap.Error(err))
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if cfg.RunTimeout > 0 {
		var timeoutCancel context.CancelFunc
		ctx, timeoutCancel = context.WithTimeout(ctx, cfg.RunTimeout)
		defer timeoutCancel()
	}

	pprofServer := startPprofServer(ctx, logger, cfg.Runtime)
	if pprofServer != nil {
		defer func() {
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer shutdownCancel()
			_ = pprofServer.Shutdown(shutdownCtx)
		}()
	}

	runner, err := app.NewRunner(cfg, logger)
	if err != nil {
		logger.Fatal("create runner", zap.Error(err))
	}

	if err := runner.Run(ctx); err != nil {
		logger.Fatal("run pipeline", zap.Error(err))
	}
}

func startPprofServer(ctx context.Context, logger *zap.Logger, cfg config.RuntimeConfig) *http.Server {
	if !cfg.PprofEnabled {
		return nil
	}

	server := &http.Server{Addr: cfg.PprofAddr}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	go func() {
		logger.Info("pprof server enabled", zap.String("addr", cfg.PprofAddr))
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("pprof server failed", zap.Error(err))
		}
	}()

	return server
}
