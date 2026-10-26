package test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"relay/internal/consumer"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func committedOffset(ctx context.Context, client *kgo.Client, group string, partition int32) (int64, error) {
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Group = group
	req.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: "relay.events", Partitions: []int32{partition}}}
	g := kmsg.NewOffsetFetchRequestGroup()
	g.Group = group
	g.Topics = []kmsg.OffsetFetchRequestGroupTopic{{Topic: "relay.events", Partitions: []int32{partition}}}
	req.Groups = []kmsg.OffsetFetchRequestGroup{g}
	resp, err := req.RequestWith(ctx, client)
	if err != nil {
		return -1, err
	}
	if resp.ErrorCode != 0 {
		return -1, fmt.Errorf("offset fetch error %d", resp.ErrorCode)
	}
	for _, group := range resp.Groups {
		if group.ErrorCode != 0 {
			return -1, fmt.Errorf("group offset fetch error %d", group.ErrorCode)
		}
		for _, topic := range group.Topics {
			for _, p := range topic.Partitions {
				if p.Partition == partition && p.ErrorCode == 0 {
					return p.Offset, nil
				}
			}
		}
	}
	for _, topic := range resp.Topics {
		for _, p := range topic.Partitions {
			if p.Partition == partition && p.ErrorCode == 0 {
				return p.Offset, nil
			}
		}
	}
	return -1, nil
}

func TestKafkaConsumerGroupEndToEnd(t *testing.T) {
	brokers := os.Getenv("TEST_KAFKA_ADDR")
	if brokers == "" {
		t.Skip("set TEST_KAFKA_ADDR to an isolated Kafka broker with relay.events created")
	}
	s := testService(t)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(204)
	}))
	t.Cleanup(server.Close)
	id := seedRecord(t, s, "ride.completed")
	seedTarget(t, s, "ride.completed", server.URL, true)
	// A sequence survives transaction rollback, letting the test observe that
	// the consumer actually tried fanout before checking its committed offset.
	if err := s.db.Exec(`CREATE SEQUENCE fanout_attempts;
		CREATE FUNCTION block_fanout() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN PERFORM nextval('fanout_attempts'); RAISE EXCEPTION 'forced temporary database failure'; END $$;
		CREATE TRIGGER block_fanout BEFORE INSERT ON deliveries FOR EACH ROW EXECUTE FUNCTION block_fanout();`).Error; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	cfg := consumer.Config{Brokers: strings.Split(brokers, ","), Group: fmt.Sprintf("relay-test-%d", time.Now().UnixNano()),
		PollInterval: 10 * time.Millisecond, RequestTimeout: time.Second, LeaseDuration: 30 * time.Second, DBTimeout: time.Second}
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); s.RunKafka(ctx, cfg) }()
		go func() { defer wg.Done(); s.RunDeliveries(ctx, cfg) }()
	}
	t.Cleanup(func() { cancel(); wg.Wait() })
	producer, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(producer.Close)
	value := []byte(fmt.Sprintf(`{"ID":%d}`, id))
	records := []*kgo.Record{
		{Topic: "relay.events", Partition: 0, Value: value},
		{Topic: "relay.events", Partition: 0, Value: value},
		{Topic: "relay.events", Partition: 0, Value: []byte("{invalid")},
	}
	if err := producer.ProduceSync(ctx, records...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	eventually(t, 15*time.Second, func() bool {
		var attempted bool
		return s.db.Raw("SELECT is_called FROM fanout_attempts").Scan(&attempted).Error == nil && attempted
	})
	offset, err := committedOffset(ctx, producer, cfg.Group, 0)
	if err != nil {
		t.Fatal(err)
	}
	if offset > records[0].Offset {
		t.Fatalf("offset advanced despite failed database transaction: %d", offset)
	}
	var count int64
	if err := s.db.Table("consumer_fanouts").Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("receipt survived failed transaction: %d, %v", count, err)
	}
	if err := s.db.Exec("DROP TRIGGER block_fanout ON deliveries; DROP FUNCTION block_fanout()").Error; err != nil {
		t.Fatal(err)
	}
	eventually(t, 15*time.Second, func() bool {
		var job consumer.Delivery
		return s.db.Where("record_id = ?", id).First(&job).Error == nil && job.Status == consumer.DeliveryStatusDelivered
	})
	eventually(t, 10*time.Second, func() bool {
		offset, err := committedOffset(ctx, producer, cfg.Group, 0)
		return err == nil && offset == records[2].Offset+1
	})
	if err := s.db.Model(&consumer.Delivery{}).Where("record_id = ?", id).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("duplicate event created duplicate jobs: %d, %v", count, err)
	}
	if err := s.db.Table("consumer_rejections").Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("invalid message was not retained before commit: %d, %v", count, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("multiple consumers sent %d copies of one delivery", calls.Load())
	}
}
