package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

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

	runner, err := app.NewRunner(cfg, logger)
	if err != nil {
		logger.Fatal("create runner", zap.Error(err))
	}

	if err := runner.Run(ctx); err != nil {
		logger.Fatal("run pipeline", zap.Error(err))
	}
}
