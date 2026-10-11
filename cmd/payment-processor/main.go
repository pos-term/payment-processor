// Command payment-processor consumes terminal transactions from Kafka and serves the REST API.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"

	"github.com/pos-term/payment-processor/internal/config"
	"github.com/pos-term/payment-processor/internal/consumer"
	"github.com/pos-term/payment-processor/internal/httpserver"
	"github.com/pos-term/payment-processor/internal/processor"
	"github.com/pos-term/payment-processor/internal/storage"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "payment-processor:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(os.LookupEnv)
	if err != nil {
		return fmt.Errorf("invalid configuration:\n%w", err)
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Info("starting payment-processor")
	if cfg.MigrateOnStart {
		if err := storage.Migrate(cfg.PostgresDSN, log); err != nil {
			return err
		}
	}

	cons, err := consumer.New(consumer.Config{Brokers: cfg.KafkaBrokers}, log)
	if err != nil {
		return err
	}
	defer cons.Close()

	proc := processor.New(cons, log)

	// Either part failing stops the other one.
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return cons.Run(gctx, proc) })
	g.Go(func() error {
		return httpserver.Run(gctx, httpserver.New(cfg.HTTPAddr, log), cfg.ShutdownTimeout, log)
	})
	return g.Wait()
}
