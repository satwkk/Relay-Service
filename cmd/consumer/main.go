package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"relay/internal/consumer"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		log.Fatalf("Relay consumer failed: %v", err)
	}
}

func run(ctx context.Context) error {
	cfg, err := consumer.ConfigFromEnv()
	if err != nil {
		return err
	}
	service, err := consumer.NewService(os.Getenv("DATABASE_URL"))
	if err != nil {
		return err
	}
	defer func() {
		if err := service.Close(); err != nil {
			log.Printf("Closing consumer database: %v", err)
		}
	}()
	log.Printf("Relay consumer started; group=%s delivery poll=%s lease=%s", cfg.Group, cfg.PollInterval, cfg.LeaseDuration)
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		service.RunDeliveries(ctx, cfg)
	}()
	service.RunKafka(ctx, cfg)
	workers.Wait()
	log.Print("Relay consumer stopped")
	return nil
}
