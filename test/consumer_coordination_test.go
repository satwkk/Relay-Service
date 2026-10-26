package test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"relay/internal/consumer"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type testFixture struct {
	*consumer.Service
	db *gorm.DB
}

func testService(t *testing.T) *testFixture {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL coordination tests")
	}
	parsed, err := url.Parse(dsn)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") {
		t.Fatal("TEST_DATABASE_URL must be a PostgreSQL URL")
	}
	config := &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)}
	admin, err := gorm.Open(postgres.Open(dsn), config)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := admin.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pool.Close() })
	schema := fmt.Sprintf("relay_coordination_%d", time.Now().UnixNano())
	if err := admin.Exec("CREATE SCHEMA " + schema).Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := admin.Exec("DROP SCHEMA " + schema + " CASCADE").Error; err != nil {
			t.Error(err)
		}
	})
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	db, err := gorm.Open(postgres.Open(parsed.String()), config)
	if err != nil {
		t.Fatal(err)
	}
	testPool, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	testPool.SetMaxOpenConns(30)
	t.Cleanup(func() { testPool.Close() })
	paths, err := filepath.Glob(filepath.Join("..", "migrations", "*.up.sql"))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		source, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Exec(string(source)).Error; err != nil {
			t.Fatalf("%s: %v", path, err)
		}
	}
	service, err := consumer.NewService(parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { service.Close() })
	return &testFixture{Service: service, db: db}
}

func seedRecord(t *testing.T, s *testFixture, eventType string) int64 {
	t.Helper()
	var id int64
	err := s.db.Raw(`INSERT INTO outbox_record_models(type, idempotence_key, payload)
		VALUES (?, 'test', '{"ride_id":"ride_123"}') RETURNING id`, eventType).Row().Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func seedTarget(t *testing.T, s *testFixture, eventType, endpoint string, enabled bool) int64 {
	t.Helper()
	var subscriberID, id int64
	err := s.db.Raw(`INSERT INTO subscribers(name, webhook_url, enabled) VALUES (?, ?, ?) RETURNING id`,
		fmt.Sprint(time.Now().UnixNano()), endpoint, enabled).Row().Scan(&subscriberID)
	if err != nil {
		t.Fatal(err)
	}
	err = s.db.Raw(`INSERT INTO subscriptions(subscriber_id, event_type) VALUES (?, ?) RETURNING id`, subscriberID, eventType).Row().Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func eventually(t *testing.T, timeout time.Duration, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for asynchronous processing")
}

func TestFanoutIsAtomicAndReplaySafe(t *testing.T) {
	s := testService(t)
	ctx := context.Background()
	id := seedRecord(t, s, "ride.completed")
	seedTarget(t, s, "ride.completed", "http://payments/webhook", true)
	seedTarget(t, s, "ride.completed", "http://notifications/webhook", true)
	seedTarget(t, s, "ride.completed", "http://disabled/webhook", false)
	disabled := seedTarget(t, s, "ride.completed", "http://disabled-binding/webhook", true)
	if err := s.db.Exec("UPDATE subscriptions SET enabled = false WHERE id = ?", disabled).Error; err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errors := make(chan error, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errors <- s.Fanout(ctx, id) }()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	var count int64
	s.db.Model(&consumer.Delivery{}).Where("record_id = ?", id).Count(&count)
	if count != 2 {
		t.Fatalf("expected two active targets, got %d", count)
	}
	seedTarget(t, s, "ride.completed", "http://added-later/webhook", true)
	if err := s.Fanout(ctx, id); err != nil {
		t.Fatal(err)
	}
	s.db.Model(&consumer.Delivery{}).Where("record_id = ?", id).Count(&count)
	if count != 2 {
		t.Fatal("replay changed an already completed fanout")
	}
	empty := seedRecord(t, s, "unhandled")
	if err := s.Fanout(ctx, empty); err != nil {
		t.Fatal(err)
	}
	seedTarget(t, s, "unhandled", "http://added-later/webhook", true)
	if err := s.Fanout(ctx, empty); err != nil {
		t.Fatal(err)
	}
	s.db.Model(&consumer.Delivery{}).Where("record_id = ?", empty).Count(&count)
	if count != 0 {
		t.Fatal("empty fanout was not remembered")
	}
	broken := seedRecord(t, s, "ride.completed")
	if err := s.db.Exec(`CREATE FUNCTION fail_delivery() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'forced delivery insert failure'; END $$;
		CREATE TRIGGER fail_delivery BEFORE INSERT ON deliveries FOR EACH ROW EXECUTE FUNCTION fail_delivery();`).Error; err != nil {
		t.Fatal(err)
	}
	if err := s.Fanout(ctx, broken); err == nil {
		t.Fatal("forced insert failure was ignored")
	}
	s.db.Table("consumer_fanouts").Where("record_id = ?", broken).Count(&count)
	if count != 0 {
		t.Fatal("fanout receipt survived a rolled-back delivery insert")
	}
	if err := s.db.Exec("DROP TRIGGER fail_delivery ON deliveries; DROP FUNCTION fail_delivery()").Error; err != nil {
		t.Fatal(err)
	}
	if err := s.Fanout(ctx, broken); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentClaimsAndLeaseRecovery(t *testing.T) {
	s := testService(t)
	ctx := context.Background()
	id := seedRecord(t, s, "ride.completed")
	for i := 0; i < 20; i++ {
		seedTarget(t, s, "ride.completed", "http://payments/webhook", true)
	}
	if err := s.Fanout(ctx, id); err != nil {
		t.Fatal(err)
	}
	claims := make(chan *consumer.Delivery, 20)
	failures := make(chan error, 20)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			job, err := s.Claim(ctx, time.Minute)
			if err != nil {
				failures <- err
				return
			}
			claims <- job
		}()
	}
	wg.Wait()
	close(claims)
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	seen := map[int64]bool{}
	var original *consumer.Delivery
	for job := range claims {
		if job == nil || seen[job.ID] || job.ClaimToken == nil || job.AttemptCount != 1 {
			t.Fatalf("invalid or duplicate claim: %+v", job)
		}
		seen[job.ID] = true
		original = job
	}
	if len(seen) != 20 {
		t.Fatalf("claimed %d jobs", len(seen))
	}
	if job, err := s.Claim(ctx, time.Minute); err != nil || job != nil {
		t.Fatalf("an active lease was claimed twice: %+v, %v", job, err)
	}
	if err := s.db.Exec("UPDATE deliveries SET lease_expires_at = now() - interval '1 second' WHERE id = ?", original.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := consumer.RecordResult(ctx, s.db, *original, nil); !errors.Is(err, consumer.ErrClaimLost) {
		t.Fatalf("expired owner completed delivery: %v", err)
	}
	recovered, err := s.Claim(ctx, time.Minute)
	if err != nil || recovered == nil || recovered.ID != original.ID || recovered.AttemptCount != 2 || *recovered.ClaimToken == *original.ClaimToken {
		t.Fatalf("invalid lease recovery: %+v, %v", recovered, err)
	}
	if err := consumer.RecordResult(ctx, s.db, *original, nil); !errors.Is(err, consumer.ErrClaimLost) {
		t.Fatal("stale owner overwrote new claim")
	}
	if err := consumer.RecordResult(ctx, s.db, *recovered, errors.New("HTTP 503")); err != nil {
		t.Fatal(err)
	}
	if job, err := s.Claim(ctx, time.Minute); err != nil || job != nil {
		t.Fatal("retry backoff was ignored")
	}
	if err := s.db.Exec("UPDATE deliveries SET next_attempt_at = now() WHERE id = ?", recovered.ID).Error; err != nil {
		t.Fatal(err)
	}
	final, err := s.Claim(ctx, time.Minute)
	if err != nil || final == nil || final.AttemptCount != 3 {
		t.Fatalf("retry claim: %+v, %v", final, err)
	}
	if err := s.db.Exec("UPDATE deliveries SET attempt_count = ?, lease_expires_at = now() - interval '1 second' WHERE id = ?", consumer.MaxDeliveryAttempts, final.ID).Error; err != nil {
		t.Fatal(err)
	}
	exhausted, err := s.Claim(ctx, time.Minute)
	if err != nil || exhausted == nil || exhausted.Status != consumer.DeliveryStatusFailed || exhausted.AttemptCount != consumer.MaxDeliveryAttempts || exhausted.ClaimToken != nil {
		t.Fatalf("attempt limit after crash: %+v, %v", exhausted, err)
	}
	// Resolve remaining claims so the down migration can safely remove leases.
	if err := s.db.Exec("UPDATE deliveries SET status = 'failed', claim_token = NULL, lease_expires_at = NULL WHERE status = 'processing'").Error; err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"000005_coordinate_consumers.down.sql", "000005_coordinate_consumers.up.sql"} {
		source, err := os.ReadFile(filepath.Join("..", "migrations", name))
		if err != nil {
			t.Fatal(err)
		}
		if err := s.db.Exec(string(source)).Error; err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

func TestDeliveryRetriesWithoutKafka(t *testing.T) {
	s := testService(t)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if string(body) != `{"ride_id":"ride_123"}` || r.Header.Get("X-Relay-consumer.Delivery-ID") == "" {
			t.Error("webhook lost its JSON payload or delivery identity")
		}
		if calls.Add(1) == 1 {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(204)
	}))
	t.Cleanup(server.Close)
	id := seedRecord(t, s, "ride.completed")
	seedTarget(t, s, "ride.completed", server.URL, true)
	if err := s.Fanout(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cfg := consumer.Config{PollInterval: 10 * time.Millisecond, RequestTimeout: time.Second, LeaseDuration: 30 * time.Second, DBTimeout: time.Second}
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); s.RunDeliveries(ctx, cfg) }()
	}
	t.Cleanup(func() { cancel(); wg.Wait() })
	eventually(t, 3*time.Second, func() bool {
		var job consumer.Delivery
		return s.db.Where("record_id = ?", id).First(&job).Error == nil && job.Status == consumer.DeliveryStatusPending && job.AttemptCount == 1 && job.LastError != nil && strings.Contains(*job.LastError, "503")
	})
	if err := s.db.Exec("UPDATE deliveries SET next_attempt_at = now() WHERE record_id = ?", id).Error; err != nil {
		t.Fatal(err)
	}
	eventually(t, 3*time.Second, func() bool {
		var job consumer.Delivery
		return s.db.Where("record_id = ?", id).First(&job).Error == nil && job.Status == consumer.DeliveryStatusDelivered && job.AttemptCount == 2 && job.LastError == nil && job.DeliveredAt != nil
	})
	if calls.Load() != 2 {
		t.Fatalf("expected one failure and one success, got %d requests", calls.Load())
	}
}
