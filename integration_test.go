package memcache

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

func integrationClient(t *testing.T, options ...Option) *Client {
	t.Helper()
	all := append([]Option{WithTimeout(2 * time.Second)}, options...)
	client, err := New(startMemcached(t), all...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestIntegrationObjectCache(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	value := []byte("a\r\n\x00b")

	if _, ok, err := c.Get(ctx, "binary key"); err != nil || ok {
		t.Fatalf("miss: ok=%v err=%v", ok, err)
	}
	if err := c.Set(ctx, "binary key", nil, time.Minute); err == nil {
		t.Fatal("empty value was accepted")
	}
	if err := c.Set(ctx, "binary key", value, time.Minute); err != nil {
		t.Fatal(err)
	}
	// Forever is the explicit "never expire" choice; memcached reports it as a
	// negative remaining TTL.
	if err := c.Set(ctx, "eternal", []byte("e"), Forever); err != nil {
		t.Fatal(err)
	}
	if info, ok, err := c.Inspect(ctx, "eternal"); err != nil || !ok || info.TTL >= 0 {
		t.Fatalf("forever entry: %#v ok=%v err=%v", info, ok, err)
	}
	got, ok, err := c.Get(ctx, "binary key")
	if err != nil || !ok || string(got) != string(value) {
		t.Fatalf("hit: %q ok=%v err=%v", got, ok, err)
	}

	if won, err := c.Add(ctx, "binary key", []byte("other"), time.Minute); err != nil || won {
		t.Fatalf("add existing = %v, %v", won, err)
	}
	if won, err := c.Add(ctx, "job", []byte("1"), time.Minute); err != nil || !won {
		t.Fatalf("add new = %v, %v", won, err)
	}
	if alive, err := c.Replace(ctx, "nobody", []byte("x"), time.Minute); err != nil || alive {
		t.Fatalf("replace missing = %v, %v", alive, err)
	}
	if alive, err := c.Replace(ctx, "job", []byte("2"), time.Minute); err != nil || !alive {
		t.Fatalf("replace existing = %v, %v", alive, err)
	}

	info, ok, err := c.Inspect(ctx, "binary key")
	if err != nil || !ok {
		t.Fatalf("inspect: ok=%v err=%v", ok, err)
	}
	if info.Size != len(value) || info.TTL <= 0 || info.TTL > time.Minute {
		t.Fatalf("inspect metadata: %#v", info)
	}
	if _, ok, err := c.Inspect(ctx, "nobody"); err != nil || ok {
		t.Fatalf("inspect missing: ok=%v err=%v", ok, err)
	}

	if err := c.Delete(ctx, "binary key"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := c.Get(ctx, "binary key"); ok {
		t.Fatal("deleted key still readable")
	}
	if err := c.Delete(ctx, "binary key"); err != nil {
		t.Fatalf("idempotent delete: %v", err)
	}
}

func TestIntegrationSessionRenewal(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	const sessionTTL = time.Minute

	if _, ok, err := c.Get(ctx, "session:1", Touch(sessionTTL)); err != nil || ok {
		t.Fatalf("expired session: ok=%v err=%v", ok, err)
	}
	if err := c.Set(ctx, "session:1", []byte("s"), 2*time.Second); err != nil {
		t.Fatal(err)
	}
	got, ok, err := c.Get(ctx, "session:1", Touch(sessionTTL))
	if err != nil || !ok || string(got) != "s" {
		t.Fatalf("get with touch: %q ok=%v err=%v", got, ok, err)
	}
	info, ok, err := c.Inspect(ctx, "session:1")
	if err != nil || !ok || info.TTL <= 2*time.Second {
		t.Fatalf("TTL did not slide: %#v ok=%v err=%v", info, ok, err)
	}

	if err := c.Touch(ctx, "session:1", time.Hour); err != nil {
		t.Fatal(err)
	}
	info, _, _ = c.Inspect(ctx, "session:1")
	if info.TTL <= sessionTTL {
		t.Fatalf("touch did not extend: %#v", info)
	}
	if err := c.Touch(ctx, "session:gone", time.Hour); err != nil {
		t.Fatalf("touch missing: %v", err)
	}
}

func TestIntegrationCollections(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	mapping := map[string][]byte{"a": []byte("A"), "b": []byte("B"), "c": []byte("C")}
	if err := c.SetMany(ctx, mapping, time.Minute); err != nil {
		t.Fatal(err)
	}
	found, err := c.GetMany(ctx, []string{"a", "b", "c", "missing"})
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 3 || string(found["a"]) != "A" || string(found["c"]) != "C" {
		t.Fatalf("get many: %#v", found)
	}
	if _, present := found["missing"]; present {
		t.Fatal("miss did not stay absent")
	}
	if err := c.DeleteMany(ctx, []string{"a", "b", "not-there"}); err != nil {
		t.Fatal(err)
	}
	found, err = c.GetMany(ctx, []string{"a", "b", "c"})
	if err != nil || len(found) != 1 || string(found["c"]) != "C" {
		t.Fatalf("after delete many: %#v, %v", found, err)
	}
}

func TestIntegrationFetchMissPath(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	var calls atomic.Int32
	loader := func(context.Context) ([]byte, error) {
		calls.Add(1)
		time.Sleep(50 * time.Millisecond)
		return []byte("report"), nil
	}

	const waiters = 8
	results := make([][]byte, waiters)
	errs := make([]error, waiters)
	var wg sync.WaitGroup
	for i := range waiters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], errs[i] = c.Fetch(ctx, "report:q3", time.Minute, loader)
		}()
	}
	wg.Wait()
	for i := range waiters {
		if errs[i] != nil || string(results[i]) != "report" {
			t.Fatalf("waiter %d: %q, %v", i, results[i], errs[i])
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("loader ran %d times, want 1", calls.Load())
	}
	if _, err := c.Fetch(ctx, "report:q3", time.Minute, loader); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatal("a fresh hit ran the loader")
	}
}

func TestIntegrationFetchLoaderError(t *testing.T) {
	c := integrationClient(t)
	boom := errors.New("upstream down")
	_, err := c.Fetch(context.Background(), "broken", time.Minute, func(context.Context) ([]byte, error) {
		return nil, boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("loader error was not passed through: %v", err)
	}
	// The failed winner must not have cached anything readable.
	if _, ok, err := c.Get(context.Background(), "broken"); err != nil || ok {
		t.Fatalf("after failed load: ok=%v err=%v", ok, err)
	}
}

func TestIntegrationFetchRefreshAhead(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	if err := c.Set(ctx, "feed", []byte("old"), 2*time.Second); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	loader := func(context.Context) ([]byte, error) {
		calls.Add(1)
		return []byte("new"), nil
	}
	// Remaining TTL (2s) is inside the refresh window, so this caller wins the
	// recache lease, gets the current value with zero added latency, and the
	// refresh happens behind it.
	got, err := c.Fetch(ctx, "feed", time.Minute, loader, RefreshAhead(30*time.Second))
	if err != nil || string(got) != "old" {
		t.Fatalf("refresh-ahead fetch: %q, %v", got, err)
	}
	waitFor(t, func() bool {
		value, ok, err := c.Get(ctx, "feed")
		return err == nil && ok && string(value) == "new"
	}, "background refresh never landed")
	if calls.Load() != 1 {
		t.Fatalf("loader ran %d times, want 1", calls.Load())
	}
}

func TestIntegrationInvalidateGrace(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	if err := c.Set(ctx, "article", []byte("v1"), time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := c.Invalidate(ctx, "article", time.Minute); err != nil {
		t.Fatal(err)
	}
	// Soft invalidation means plain readers keep the old copy, unmarked. The
	// server offers this first reader the recache token; a loaderless Get
	// hands it back in the background so Fetch can still elect.
	got, ok, err := c.Get(ctx, "article")
	if err != nil || !ok || string(got) != "v1" {
		t.Fatalf("stale read: %q ok=%v err=%v", got, ok, err)
	}
	// Fetch keeps serving the old copy while one elected caller recomputes
	// behind it; the refresh eventually lands and readers switch to v2.
	sawOld := false
	waitFor(t, func() bool {
		value, err := c.Fetch(ctx, "article", time.Minute, func(context.Context) ([]byte, error) {
			return []byte("v2"), nil
		})
		if err != nil {
			t.Fatal(err)
		}
		sawOld = sawOld || string(value) == "v1"
		return string(value) == "v2"
	}, "grace-period refresh never landed")
	if !sawOld {
		t.Fatal("no reader was served the stale copy during the grace period")
	}

	if err := c.Invalidate(ctx, "no-such-key", time.Minute); err != nil {
		t.Fatalf("invalidate missing: %v", err)
	}
}

func TestIntegrationUpdate(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()

	// A miss starts the transform from (nil, false).
	got, err := c.Update(ctx, "cart", time.Minute, func(current []byte, found bool) ([]byte, error) {
		if found || current != nil {
			return nil, fmt.Errorf("unexpected state: %q %v", current, found)
		}
		return []byte("1"), nil
	})
	if err != nil || string(got) != "1" {
		t.Fatalf("miss update: %q, %v", got, err)
	}

	// Concurrent updates must all land exactly once via the retry loop. The
	// writer count stays under the retry bound so the worst-case loser still
	// succeeds.
	const writers = 6
	var wg sync.WaitGroup
	errs := make([]error, writers)
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = c.Update(ctx, "cart", time.Minute, func(current []byte, found bool) ([]byte, error) {
				if !found {
					return nil, fmt.Errorf("value vanished")
				}
				n, err := strconv.Atoi(string(current))
				if err != nil {
					return nil, err
				}
				return []byte(strconv.Itoa(n + 1)), nil
			})
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("writer %d: %v", i, err)
		}
	}
	final, ok, err := c.Get(ctx, "cart")
	if err != nil || !ok || string(final) != strconv.Itoa(writers+1) {
		t.Fatalf("final = %q ok=%v err=%v", final, ok, err)
	}

	// An error from fn aborts the operation without writing.
	abort := errors.New("do not create")
	if _, err := c.Update(ctx, "never", time.Minute, func([]byte, bool) ([]byte, error) {
		return nil, abort
	}); !errors.Is(err, abort) {
		t.Fatalf("fn error was not passed through: %v", err)
	}
	if _, ok, _ := c.Get(ctx, "never"); ok {
		t.Fatal("aborted update wrote a value")
	}

	// A value in its Invalidate grace period reads as a miss: fn must not
	// launder stale data back to fresh.
	if err := c.Invalidate(ctx, "cart", time.Minute); err != nil {
		t.Fatal(err)
	}
	got, err = c.Update(ctx, "cart", time.Minute, func(current []byte, found bool) ([]byte, error) {
		if found {
			return nil, fmt.Errorf("stale value leaked into fn: %q", current)
		}
		return []byte("fresh"), nil
	})
	if err != nil || string(got) != "fresh" {
		t.Fatalf("stale update: %q, %v", got, err)
	}
	if value, ok, _ := c.Get(ctx, "cart"); !ok || string(value) != "fresh" {
		t.Fatalf("after stale update: %q ok=%v", value, ok)
	}
}

func TestIntegrationCounters(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()

	if _, err := c.Incr(ctx, "rate", 1, -time.Second); err == nil || !strings.Contains(err.Error(), "negative") {
		t.Fatalf("incr with a negative ttl: %v", err)
	}
	n, err := c.Incr(ctx, "rate", 1, time.Minute)
	if err != nil || n != 1 {
		t.Fatalf("first increment = %d, %v", n, err)
	}
	n, err = c.Incr(ctx, "rate", 4, time.Minute)
	if err != nil || n != 5 {
		t.Fatalf("second increment = %d, %v", n, err)
	}
	n, err = c.Decr(ctx, "rate", 100, time.Minute)
	if err != nil || n != 0 {
		t.Fatalf("decrement did not saturate: %d, %v", n, err)
	}
	n, err = c.Decr(ctx, "fresh-floor", 3, time.Minute)
	if err != nil || n != 0 {
		t.Fatalf("decrement on miss = %d, %v", n, err)
	}
}

func TestIntegrationAppendTake(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()

	if err := c.Append(ctx, "events", []byte("login;"), -time.Second); err == nil || !strings.Contains(err.Error(), "negative") {
		t.Fatalf("append with a negative ttl: %v", err)
	}
	if err := c.Append(ctx, "events", []byte("login;"), time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := c.Append(ctx, "events", []byte("click;"), time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := c.Prepend(ctx, "events", []byte("boot;"), time.Hour); err != nil {
		t.Fatal(err)
	}

	got, ok, err := c.Get(ctx, "events")
	if err != nil || !ok || string(got) != "boot;login;click;" {
		t.Fatalf("get: %q ok=%v err=%v", got, ok, err)
	}
	taken, err := c.Take(ctx, "events")
	if err != nil || string(taken) != "boot;login;click;" {
		t.Fatalf("take: %q, %v", taken, err)
	}
	if taken, err = c.Take(ctx, "events"); err != nil || taken != nil {
		t.Fatalf("take empty: %q, %v", taken, err)
	}
	if _, ok, _ := c.Get(ctx, "events"); ok {
		t.Fatal("take left data behind")
	}
}

func TestIntegrationMetaLayer(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	meta := c.Meta()

	stored, err := meta.Set(ctx, "metadata", []byte("abc"), MetaSetOptions{ReturnCAS: true, ReturnSize: true, ReturnKey: true, Opaque: "request-1"})
	if err != nil {
		t.Fatal(err)
	}
	if stored.CAS == nil || stored.Size == nil || *stored.Size != 3 || string(stored.ReturnedKey) != "metadata" || stored.Opaque != "request-1" {
		t.Fatalf("store response metadata: %#v", stored)
	}
	got, err := meta.Get(ctx, "metadata", MetaGetOptions{ReturnCAS: true})
	if err != nil || got.Status != GetHit || string(got.Value) != "abc" || got.Metadata.CAS == nil {
		t.Fatalf("meta get: %#v, %v", got, err)
	}
	staleFor := Expiration(60)
	invalidated, err := meta.Delete(ctx, "metadata", MetaDeleteOptions{Invalidate: true, StaleFor: &staleFor})
	if err != nil || !invalidated.Applied() {
		t.Fatalf("invalidate: %#v, %v", invalidated, err)
	}
	stale, err := meta.Get(ctx, "metadata", MetaGetOptions{})
	if err != nil || stale.Status != GetHit || stale.ValueState != ValueStale {
		t.Fatalf("stale get: %#v, %v", stale, err)
	}

	initial, ttl := uint64(10), Expiration(60)
	counter, err := meta.Arithmetic(ctx, "counter", MetaArithmeticOptions{Delta: 2, Initial: &initial, InitialTTL: &ttl})
	if err != nil || !counter.HasValue || counter.Value != 10 {
		t.Fatalf("arithmetic: %#v, %v", counter, err)
	}

	results, err := meta.Batch(ctx, []Operation{
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
	if !results[0].Mutation.Applied() || results[1].Get.Status != GetMiss || string(results[2].Get.Value) != "A" ||
		!results[3].Mutation.Applied() || results[4].Mutation.Status != MutationNotFound {
		t.Fatalf("batch results: %#v", results)
	}

	key := "a b"
	if err := c.Set(ctx, key, []byte("x"), time.Minute); err != nil {
		t.Fatal(err)
	}
	command, err := buildGet(key, MetaGetOptions{ReturnKey: true})
	if err != nil {
		t.Fatal(err)
	}
	response, err := meta.Execute(ctx, command)
	if err != nil || string(response.Key) != key {
		t.Fatalf("returned key %q, %v", response.Key, err)
	}

	if err := meta.Noop(ctx); err != nil {
		t.Fatal(err)
	}
	debug, err := meta.Debug(ctx, "metadata")
	if err != nil || debug["cas"] == "" {
		t.Fatalf("debug = %#v, %v", debug, err)
	}
}

func TestIntegrationContextCancellation(t *testing.T) {
	c := integrationClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := c.Get(ctx, "key")
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
	err = client.SetMany(context.Background(), map[string][]byte{
		"apple":  []byte("ok"),
		"banana": []byte("unreachable"),
	}, time.Minute)
	if err == nil {
		t.Fatal("unreachable shard did not report an error")
	}
	got, ok, err := client.Get(context.Background(), "apple")
	if err != nil || !ok || string(got) != "ok" {
		t.Fatalf("routed read: %q ok=%v err=%v", got, ok, err)
	}
	// With Degrade the failing shard's keys just go missing from the result.
	degraded, err := NewServers([]string{good, "127.0.0.1:1"},
		WithRouter(testRouter{}), WithDialTimeout(100*time.Millisecond), Degrade(true))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = degraded.Close() })
	found, err := degraded.GetMany(context.Background(), []string{"apple", "banana"})
	if err != nil || len(found) != 1 || string(found["apple"]) != "ok" {
		t.Fatalf("degraded get many: %#v, %v", found, err)
	}
}

// The zero-byte rule: memcached represents lease placeholders as zero-byte
// items, so Client reads fold an empty value into a miss instead of leaking
// coordination internals as a normal hit.
func TestIntegrationZeroByteValueReadsAsMiss(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	if _, err := c.Meta().Set(ctx, "empty", []byte{}, MetaSetOptions{TTL: 60}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := c.Get(ctx, "empty"); err != nil || ok {
		t.Fatalf("zero-byte get: ok=%v err=%v", ok, err)
	}
	if v, err := c.Take(ctx, "empty"); err != nil || v != nil {
		t.Fatalf("zero-byte take: %q, %v", v, err)
	}
	// Update's read step folds it too, and its conditional write can still
	// replace the placeholder without an add/exists livelock.
	got, err := c.Update(ctx, "empty", time.Minute, func(current []byte, found bool) ([]byte, error) {
		if found || current != nil {
			return nil, fmt.Errorf("placeholder leaked into fn: %q %v", current, found)
		}
		return []byte("real"), nil
	})
	if err != nil || string(got) != "real" {
		t.Fatalf("update over placeholder: %q, %v", got, err)
	}
}

// A caller that loses the vivify lease to another process waits briefly, then
// computes locally without writing back.
func TestIntegrationFetchBusyLeaseFallsBack(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	// Another process grabs the lease first.
	lease := Expiration(30)
	foreign, err := c.Meta().Get(ctx, "busy", MetaGetOptions{VivifyTTL: &lease})
	if err != nil || foreign.Lease != LeaseGranted {
		t.Fatalf("foreign lease: %#v, %v", foreign, err)
	}
	var calls atomic.Int32
	value, err := c.Fetch(ctx, "busy", time.Minute, func(context.Context) ([]byte, error) {
		calls.Add(1)
		return []byte("local"), nil
	})
	if err != nil || string(value) != "local" {
		t.Fatalf("losing fetch: %q, %v", value, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("local fallback ran the loader %d times", calls.Load())
	}
	// The loser must not have written back: the key still reads as a miss.
	if _, ok, err := c.Get(ctx, "busy"); err != nil || ok {
		t.Fatalf("loser wrote back: ok=%v err=%v", ok, err)
	}
}

// Write-back is conditional on the CAS from the winning read: a key deleted
// while the loader ran must not be resurrected.
func TestIntegrationFetchWriteBackDoesNotResurrect(t *testing.T) {
	var reports atomic.Int32
	c := integrationClient(t, OnError(func(error) { reports.Add(1) }))
	ctx := context.Background()
	value, err := c.Fetch(ctx, "logout", time.Minute, func(context.Context) ([]byte, error) {
		// The user logs out while the winner is recomputing.
		if err := c.Delete(context.Background(), "logout"); err != nil {
			return nil, err
		}
		return []byte("session"), nil
	})
	if err != nil || string(value) != "session" {
		t.Fatalf("fetch: %q, %v", value, err)
	}
	if _, ok, err := c.Get(ctx, "logout"); err != nil || ok {
		t.Fatalf("dead data was resurrected: ok=%v err=%v", ok, err)
	}
	if reports.Load() == 0 {
		t.Fatal("abandoned write-back never reached OnError")
	}
}

// A writer that keeps losing the compare-and-swap race gives up with
// ErrConflict after a bounded number of attempts.
func TestIntegrationUpdateConflictExhaustsRetries(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	if err := c.Set(ctx, "contended", []byte("0"), time.Minute); err != nil {
		t.Fatal(err)
	}
	attempts := 0
	_, err := c.Update(ctx, "contended", time.Minute, func(current []byte, found bool) ([]byte, error) {
		attempts++
		// Simulate a concurrent writer landing between the read and the
		// conditional write on every attempt.
		if err := c.Set(ctx, "contended", []byte("bumped"), time.Minute); err != nil {
			return nil, err
		}
		return []byte("mine"), nil
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("got %v, want ErrConflict", err)
	}
	if attempts != updateAttempts {
		t.Fatalf("update tried %d times, want %d", attempts, updateAttempts)
	}
}

// Take retries when an append slips in between its read and its conditional
// delete, so no appended bytes are ever lost in the gap.
func TestIntegrationTakeLosesNoAppends(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	if err := c.Append(ctx, "acc", []byte("a;"), time.Hour); err != nil {
		t.Fatal(err)
	}
	const appenders = 8
	var wg sync.WaitGroup
	for range appenders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = c.Append(ctx, "acc", []byte("x;"), time.Hour)
		}()
	}
	var taken []byte
	for {
		part, err := c.Take(ctx, "acc")
		if err != nil {
			t.Fatal(err)
		}
		if part == nil {
			break
		}
		taken = append(taken, part...)
	}
	wg.Wait()
	// Collect stragglers that appended after the last take returned nil.
	rest, err := c.Take(ctx, "acc")
	if err != nil {
		t.Fatal(err)
	}
	taken = append(taken, rest...)
	if got := strings.Count(string(taken), ";"); got != appenders+1 {
		t.Fatalf("took %d fragments, want %d: %q", got, appenders+1, taken)
	}
}

// A failed lease winner releases the vivify placeholder, so the next Fetch
// re-elects and repairs the key instead of waiting out the placeholder TTL.
func TestIntegrationFetchLoaderErrorReleasesLease(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	boom := errors.New("upstream down")
	if _, err := c.Fetch(ctx, "flaky", time.Minute, func(context.Context) ([]byte, error) {
		return nil, boom
	}); !errors.Is(err, boom) {
		t.Fatalf("loader error was not passed through: %v", err)
	}
	// The upstream recovers immediately; the next Fetch must win a fresh
	// lease and write back, not fall into the no-write-back fallback.
	value, err := c.Fetch(ctx, "flaky", time.Minute, func(context.Context) ([]byte, error) {
		return []byte("recovered"), nil
	})
	if err != nil || string(value) != "recovered" {
		t.Fatalf("recovery fetch: %q, %v", value, err)
	}
	if got, ok, err := c.Get(ctx, "flaky"); err != nil || !ok || string(got) != "recovered" {
		t.Fatalf("recovery was not written back: %q ok=%v err=%v", got, ok, err)
	}
}

// Touch is protocol-native and blind: it extends whatever it hits, including
// an entry kept stale by Invalidate. The grace given to Invalidate is an
// upper bound only while nothing renews the key; a revocation that must
// stick goes through Delete.
func TestIntegrationTouchIsBlindTowardInvalidated(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	if err := c.Set(ctx, "session:9", []byte("s"), time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := c.Invalidate(ctx, "session:9", 2*time.Second); err != nil {
		t.Fatal(err)
	}
	got, ok, err := c.Get(ctx, "session:9", Touch(time.Hour))
	if err != nil || !ok || string(got) != "s" {
		t.Fatalf("stale read with touch: %q ok=%v err=%v", got, ok, err)
	}
	info, ok, err := c.Inspect(ctx, "session:9")
	if err != nil || !ok {
		t.Fatalf("inspect: ok=%v err=%v", ok, err)
	}
	if info.TTL <= 3*time.Second {
		t.Fatalf("blind touch did not extend the stale entry: %v", info.TTL)
	}
}

// A stored zero-byte value is not a lease placeholder: Fetch replaces it
// through its CAS instead of waiting for a winner that will never come.
func TestIntegrationFetchRepairsStoredEmptyValue(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	if _, err := c.Meta().Set(ctx, "hollow", []byte{}, MetaSetOptions{TTL: 60}); err != nil {
		t.Fatal(err)
	}
	value, err := c.Fetch(ctx, "hollow", time.Minute, func(context.Context) ([]byte, error) {
		return []byte("real"), nil
	})
	if err != nil || string(value) != "real" {
		t.Fatalf("fetch over empty value: %q, %v", value, err)
	}
	if got, ok, err := c.Get(ctx, "hollow"); err != nil || !ok || string(got) != "real" {
		t.Fatalf("empty value was not repaired: %q ok=%v err=%v", got, ok, err)
	}
}

// Losers whose cross-process wait fails still merge into one loader per key
// per process instead of stampeding the origin.
func TestIntegrationFetchFallbackMergesLoaders(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	// Another process grabs the lease and never delivers.
	lease := Expiration(30)
	foreign, err := c.Meta().Get(ctx, "held", MetaGetOptions{VivifyTTL: &lease})
	if err != nil || foreign.Lease != LeaseGranted {
		t.Fatalf("foreign lease: %#v, %v", foreign, err)
	}
	var calls atomic.Int32
	loader := func(context.Context) ([]byte, error) {
		calls.Add(1)
		time.Sleep(300 * time.Millisecond)
		return []byte("shared"), nil
	}
	const waiters = 8
	values := make([][]byte, waiters)
	errs := make([]error, waiters)
	var wg sync.WaitGroup
	for i := range waiters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			values[i], errs[i] = c.Fetch(ctx, "held", time.Minute, loader)
		}()
	}
	wg.Wait()
	for i := range waiters {
		if errs[i] != nil || string(values[i]) != "shared" {
			t.Fatalf("waiter %d: %q, %v", i, values[i], errs[i])
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("fallback ran %d loaders, want 1", calls.Load())
	}
}

func waitFor(t *testing.T, condition func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(message)
}
