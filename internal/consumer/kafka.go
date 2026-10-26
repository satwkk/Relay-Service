package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"relay/internal/ingestion"

	"github.com/twmb/franz-go/pkg/kgo"
)

func (s *Service) RunKafka(ctx context.Context, cfg Config) {
	for ctx.Err() == nil {
		if err := s.consumeSession(ctx, cfg); err != nil && ctx.Err() == nil {
			log.Printf("Kafka consumer session failed; restarting from committed offsets: %v", err)
		}
		if !wait(ctx, time.Second) {
			return
		}
	}
}

func (s *Service) consumeSession(ctx context.Context, cfg Config) error {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...), kgo.ConsumeTopics("relay.events"),
		kgo.ConsumerGroup(cfg.Group), kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		return err
	}
	defer func() {
		client.AllowRebalance()
		leaveCtx, cancel := context.WithTimeout(context.Background(), cfg.DBTimeout)
		_ = client.LeaveGroupContext(leaveCtx)
		cancel()
		client.Close()
	}()
	for ctx.Err() == nil {
		// Bound processing time and never commit past a failed record. Restart
		// after database failure: another poll advances the local fetch position.
		fetches := client.PollRecords(ctx, 1)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if failures := fetches.Errors(); len(failures) > 0 {
			return fmt.Errorf("fetch %s partition %d: %w", failures[0].Topic, failures[0].Partition, failures[0].Err)
		}
		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			dbCtx, cancel := context.WithTimeout(ctx, cfg.DBTimeout)
			err := s.handleRecord(dbCtx, record)
			cancel()
			if err != nil {
				return err
			}
			commitCtx, cancel := context.WithTimeout(ctx, cfg.DBTimeout)
			err = client.CommitRecords(commitCtx, record)
			cancel()
			if err != nil {
				return fmt.Errorf("commit Kafka offset: %w", err)
			}
		}
		client.AllowRebalance()
	}
	return ctx.Err()
}

func (s *Service) handleRecord(ctx context.Context, record *kgo.Record) error {
	var event ingestion.OutboxRecordModel
	err := json.Unmarshal(record.Value, &event)
	if err == nil && event.ID <= 0 {
		err = fmt.Errorf("Kafka record must contain a positive outbox ID")
	}
	if err == nil {
		if err := s.Fanout(ctx, event); err != nil {
			return fmt.Errorf("fanout outbox %d: %w", event.ID, err)
		}
		return nil
	}
	// Retain poison messages before acknowledging them, so they cannot stall a
	// partition forever or disappear without an inspectable database record.
	value := record.Value
	if value == nil {
		value = []byte{}
	}
	result := s.db.WithContext(ctx).Exec(`INSERT INTO consumer_rejections
		(topic, partition, record_offset, record_value, last_error) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT DO NOTHING`, record.Topic, record.Partition, record.Offset, value, err.Error())
	if result.Error != nil {
		return fmt.Errorf("retain rejected Kafka record: %w", result.Error)
	}
	log.Printf("Retained rejected Kafka record %s/%d/%d: %v", record.Topic, record.Partition, record.Offset, err)
	return nil
}
