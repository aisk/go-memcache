package memcache

import (
	"context"
	"errors"
	"net"
	"os/exec"
	"strconv"
	"testing"
	"time"
)

func startMemcached(t *testing.T) string {
	t.Helper()
	path, err := exec.LookPath("memcached")
	if err != nil {
		t.Skip("memcached executable is not installed")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	command := exec.Command(path, "-p", strconv.Itoa(port), "-U", "0", "-l", "127.0.0.1", "-m", "16")
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = command.Process.Kill(); _ = command.Wait() })
	address := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, dialErr := net.DialTimeout("tcp", address, 20*time.Millisecond)
		if dialErr == nil {
			_ = conn.Close()
			return address
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("memcached did not start")
	return ""
}

func integrationClient(t *testing.T) *Client {
	t.Helper()
	client, err := New(startMemcached(t), WithTimeout(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestIntegrationCoreAndMetadata(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	value := []byte("a\r\n\x00b")
	if err := c.Set(ctx, "binary key", value, ExpiresIn(time.Minute)); err != nil {
		t.Fatal(err)
	}
	got, err := c.Get(ctx, "binary key")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(value) {
		t.Fatalf("got %q, want %q", got, value)
	}
	item, err := c.Inspect(ctx, "binary key")
	if err != nil {
		t.Fatal(err)
	}
	if item.Status != GetHit || item.Metadata.CAS == nil || item.Metadata.Size == nil || *item.Metadata.Size != uint64(len(value)) {
		t.Fatalf("bad metadata: %#v", item)
	}
	added, err := c.Add(ctx, "binary key", []byte("other"), 0)
	if err != nil || added {
		t.Fatalf("add existing = %v, %v", added, err)
	}
	wrong := *item.Metadata.CAS + 1
	stored, err := c.CompareAndSwap(ctx, "binary key", []byte("other"), 0, wrong)
	if err != nil || stored {
		t.Fatalf("CAS mismatch = %v, %v", stored, err)
	}
	deleted, err := c.Delete(ctx, "binary key")
	if err != nil || !deleted {
		t.Fatalf("delete = %v, %v", deleted, err)
	}
	_, err = c.Get(ctx, "binary key")
	if !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("get deleted: %v", err)
	}
}

func TestIntegrationArithmeticLeaseStale(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	initial, ttl := uint64(10), Expiration(60)
	counter, err := c.Arithmetic(ctx, "counter", ArithmeticOptions{Delta: 2, Initial: &initial, InitialTTL: &ttl, ReturnCAS: true})
	if err != nil || !counter.HasValue {
		t.Fatalf("initialize counter: %#v, %v", counter, err)
	}
	value, err := c.Increment(ctx, "counter", 3)
	if err != nil || value < 10 {
		t.Fatalf("increment = %d, %v", value, err)
	}
	leaseTTL := Expiration(30)
	lease, err := c.GetWithOptions(ctx, "lease", GetOptions{VivifyTTL: &leaseTTL})
	if err != nil {
		t.Fatal(err)
	}
	if lease.Lease != LeaseGranted || lease.Metadata.CAS == nil {
		t.Fatalf("lease not granted: %#v", lease)
	}
	fulfilled, err := c.Store(ctx, "lease", []byte("fresh"), SetOptions{TTL: ttl, CompareCAS: lease.Metadata.CAS})
	if err != nil || !fulfilled.Applied() {
		t.Fatalf("fulfill lease: %#v, %v", fulfilled, err)
	}
	invalidated, err := c.DeleteWithOptions(ctx, "lease", DeleteOptions{Invalidate: true, StaleFor: &ttl})
	if err != nil || !invalidated.Applied() {
		t.Fatalf("invalidate: %#v, %v", invalidated, err)
	}
	stale, err := c.GetWithOptions(ctx, "lease", GetOptions{})
	if err != nil || stale.Status != GetHit || stale.ValueState != ValueStale {
		t.Fatalf("stale get: %#v, %v", stale, err)
	}
}

func TestIntegrationBatchAndCodec(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	type document struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}
	want := document{Name: "meta", Count: 2}
	if err := c.SetValue(ctx, "json", want, 0); err != nil {
		t.Fatal(err)
	}
	got, err := GetAs[document](ctx, c, "json")
	if err != nil || got != want {
		t.Fatalf("typed get = %#v, %v", got, err)
	}
	results, err := c.Batch(ctx, []Operation{
		SetOperation{Key: "a", Value: []byte("A")}, GetOperation{Key: "missing"},
		GetOperation{Key: "a"}, DeleteOperation{Key: "a"}, DeleteOperation{Key: "not-there"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, result := range results {
		if result.Err != nil {
			t.Fatalf("result %d: %v", i, result.Err)
		}
	}
	if results[0].Mutation == nil || !results[0].Mutation.Applied() {
		t.Fatalf("set: %#v", results[0])
	}
	if results[1].Get == nil || results[1].Get.Status != GetMiss {
		t.Fatalf("miss: %#v", results[1])
	}
	if results[2].Get == nil || string(results[2].Get.Value) != "A" {
		t.Fatalf("hit: %#v", results[2])
	}
	if results[3].Mutation == nil || !results[3].Mutation.Applied() {
		t.Fatalf("delete: %#v", results[3])
	}
	if results[4].Mutation == nil || results[4].Mutation.Status != MutationNotFound {
		t.Fatalf("delete miss: %#v", results[4])
	}
	if err := c.Noop(ctx); err != nil {
		t.Fatal(err)
	}
	debug, err := c.Debug(ctx, "json")
	if err != nil || debug["cas"] == "" {
		t.Fatalf("debug = %#v, %v", debug, err)
	}
}

func TestIntegrationContextCancellation(t *testing.T) {
	c := integrationClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.Get(ctx, "key")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}
