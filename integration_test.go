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
	storedMeta, err := c.Store(ctx, "metadata", []byte("abc"), SetOptions{ReturnCAS: true, ReturnSize: true, ReturnKey: true, Opaque: "request-1"})
	if err != nil {
		t.Fatal(err)
	}
	if storedMeta.CAS == nil || storedMeta.Size == nil || *storedMeta.Size != 3 || string(storedMeta.ReturnedKey) != "metadata" || storedMeta.Opaque != "request-1" {
		t.Fatalf("store response metadata: %#v", storedMeta)
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
	unchanged, err := c.Increment(ctx, "counter", 0)
	if err != nil || unchanged != value {
		t.Fatalf("zero delta changed counter: %d -> %d (%v)", value, unchanged, err)
	}
	leaseTTL := Expiration(30)
	lease, err := c.GetWithOptions(ctx, "lease", GetOptions{VivifyTTL: &leaseTTL, MetadataOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if lease.Lease != LeaseGranted || lease.Metadata.CAS == nil {
		t.Fatalf("lease not granted: %#v", lease)
	}
	fulfilled, err := c.FulfillLease(ctx, lease, []byte("fresh"), ttl)
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
	if err := c.Set(ctx, "early-lease", []byte("old"), ttl); err != nil {
		t.Fatal(err)
	}
	refresh := Expiration(120)
	early, err := c.GetWithOptions(ctx, "early-lease", GetOptions{RefreshBefore: &refresh})
	if err != nil || early.Status != GetHit || early.Lease != LeaseGranted || early.Metadata.CAS == nil {
		t.Fatalf("R-only early refresh: %#v, %v", early, err)
	}
	refreshed, err := c.FulfillLease(ctx, early, []byte("refreshed"), ttl)
	if err != nil || !refreshed.Applied() {
		t.Fatalf("fulfill R-only early refresh lease: %#v, %v", refreshed, err)
	}
	got, err := c.Get(ctx, "early-lease")
	if err != nil || string(got) != "refreshed" {
		t.Fatalf("get refreshed lease value: %q, %v", got, err)
	}
}

func TestIntegrationAppendPrepend(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	if err := c.Set(ctx, "concat", []byte("b"), 0); err != nil {
		t.Fatal(err)
	}
	if ok, err := c.Append(ctx, "concat", []byte("c")); err != nil || !ok {
		t.Fatalf("append = %v, %v", ok, err)
	}
	if ok, err := c.Prepend(ctx, "concat", []byte("a")); err != nil || !ok {
		t.Fatalf("prepend = %v, %v", ok, err)
	}
	got, err := c.Get(ctx, "concat")
	if err != nil || string(got) != "abc" {
		t.Fatalf("get = %q, %v", got, err)
	}
}

func TestIntegrationUnicodeWhitespaceKey(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	key := "a\u00a0b"
	if err := c.Set(ctx, key, []byte("x"), 0); err != nil {
		t.Fatal(err)
	}
	command, err := buildGet(key, GetOptions{ReturnKey: true})
	if err != nil {
		t.Fatal(err)
	}
	response, err := c.ExecuteMeta(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	if string(response.Key) != key {
		t.Fatalf("returned key %q, want %q", response.Key, key)
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
		&SetOperation{Key: "a", Value: []byte("A")}, &GetOperation{Key: "missing"},
		&GetOperation{Key: "a"}, &DeleteOperation{Key: "a"}, DeleteOperation{Key: "not-there"},
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

func TestIntegrationCodecFlag(t *testing.T) {
	address := startMemcached(t)
	client, err := New(address, WithCodec(JSONCodec{Flag: 37}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	type value struct {
		N int `json:"n"`
	}
	if err := client.SetValue(context.Background(), "flagged", value{N: 7}, 0); err != nil {
		t.Fatal(err)
	}
	got, err := GetAs[value](context.Background(), client, "flagged")
	if err != nil || got.N != 7 {
		t.Fatalf("got %#v, %v", got, err)
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

type testRouter struct{}

func (testRouter) Pick(key string, servers []string) int {
	if len(key) > 0 && key[0] == 'b' {
		return 1
	}
	return 0
}

func TestIntegrationMultiServerPartialFailure(t *testing.T) {
	good := startMemcached(t)
	client, err := NewServers([]string{good, "127.0.0.1:1"}, WithRouter(testRouter{}), WithDialTimeout(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	results, err := client.Batch(context.Background(), []Operation{
		SetOperation{Key: "apple", Value: []byte("ok")},
		SetOperation{Key: "banana", Value: []byte("unreachable")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Err != nil || results[0].Mutation == nil || !results[0].Mutation.Applied() {
		t.Fatalf("good shard: %#v", results[0])
	}
	if results[1].Err == nil || results[1].Ambiguous {
		t.Fatalf("bad shard: %#v", results[1])
	}
	got, err := client.Get(context.Background(), "apple")
	if err != nil || string(got) != "ok" {
		t.Fatalf("routed read: %q, %v", got, err)
	}
}
