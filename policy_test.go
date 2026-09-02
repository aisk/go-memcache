package memcache

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Compile-time proof of the option placement matrix: each policy option
// implements exactly the verb interfaces the design table grants it.
var (
	_ PolicyOption = RefreshAhead(time.Second)
	_ FetchOption  = RefreshAhead(time.Second)

	_ PolicyOption = Degrade(true)
	_ PolicyOption = OnError(func(error) {})

	_ Option = WithTimeout(time.Second)
)

func TestEngineOptionsAreNotPolicyOptions(t *testing.T) {
	if _, ok := any(WithTimeout(time.Second)).(PolicyOption); ok {
		t.Fatal("an engine option must not double as a policy default")
	}
}

func TestPolicyResolutionErrors(t *testing.T) {
	client, err := New("unused")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	loader := func(context.Context) ([]byte, error) { return []byte("x"), nil }
	transform := func([]byte, bool) ([]byte, error) { return []byte("x"), nil }
	negativeTTL := map[string]func() error{
		"Set":       func() error { return client.Set(ctx, "k", []byte("v"), -time.Second) },
		"SetMany":   func() error { return client.SetMany(ctx, map[string][]byte{"k": []byte("v")}, -time.Second) },
		"Add":       func() error { _, err := client.Add(ctx, "k", []byte("v"), -time.Second); return err },
		"Replace":   func() error { _, err := client.Replace(ctx, "k", []byte("v"), -time.Second); return err },
		"Fetch":     func() error { _, err := client.Fetch(ctx, "k", -time.Second, loader); return err },
		"Update":    func() error { _, err := client.Update(ctx, "k", -time.Second, transform); return err },
		"Get+Touch": func() error { _, _, err := client.Get[[]byte](ctx, "k", Touch(-time.Second)); return err },
		"GetMany+Touch": func() error {
			_, err := client.GetMany[[]byte](ctx, []string{"k"}, Touch(-time.Second))
			return err
		},
		"Touch": func() error { return client.Touch(ctx, "k", -time.Second) },
	}
	for name, call := range negativeTTL {
		if err := call(); err == nil || !strings.Contains(err.Error(), "negative") {
			t.Errorf("%s with a negative TTL: %v", name, err)
		}
	}
	negativeCreateTTL := map[string]func() error{
		"Incr":    func() error { _, err := client.Incr(ctx, "k", 1, -time.Second); return err },
		"Decr":    func() error { _, err := client.Decr(ctx, "k", 1, -time.Second); return err },
		"Append":  func() error { return client.Append(ctx, "k", []byte("v"), -time.Second) },
		"Prepend": func() error { return client.Prepend(ctx, "k", []byte("v"), -time.Second) },
	}
	for name, call := range negativeCreateTTL {
		if err := call(); err == nil || !strings.Contains(err.Error(), "negative") {
			t.Errorf("%s with a negative TTL: %v", name, err)
		}
	}
}

func TestConstructorRejectsInvalidPolicyDefaults(t *testing.T) {
	for name, option := range map[string]Option{
		"zero RefreshAhead": RefreshAhead(0),
		"nil OnError":       OnError(nil),
	} {
		if _, err := New("unused", option); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

func TestEmptyValuesAreRejectedByWrites(t *testing.T) {
	client, err := New("unused")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	writes := map[string]func() error{
		"Set":     func() error { return client.Set(ctx, "k", []byte(nil), time.Minute) },
		"SetMany": func() error { return client.SetMany(ctx, map[string][]byte{"k": {}}, time.Minute) },
		"Add":     func() error { _, err := client.Add(ctx, "k", []byte(nil), time.Minute); return err },
		"Replace": func() error { _, err := client.Replace(ctx, "k", []byte(nil), time.Minute); return err },
		"Append":  func() error { return client.Append(ctx, "k", nil, time.Minute) },
		"Prepend": func() error { return client.Prepend(ctx, "k", nil, time.Minute) },
	}
	for name, call := range writes {
		if err := call(); err == nil || !strings.Contains(err.Error(), "empty") {
			t.Errorf("%s accepted an empty value: %v", name, err)
		}
	}
}

// Degrade never absorbs an ambiguous write: degrading covers "the cache is
// unavailable", not "the write may or may not have landed".
func TestDegradeNeverAbsorbsAmbiguousWrites(t *testing.T) {
	client, err := New("unused", Degrade(true))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	ambiguous := &AmbiguousWriteError{Operation: "ms", Key: "k", Cause: errors.New("connection lost")}
	if client.absorb(ctx, ambiguous) {
		t.Fatal("ambiguous write was absorbed")
	}
	if !client.absorb(ctx, errors.New("cache down")) {
		t.Fatal("plain infrastructure failure was not absorbed")
	}
}

// Degrade absorbs only infrastructure failures: a permanent client-side
// mistake or the caller's own cancellation must surface instead of turning
// into a silent fake result on every call.
func TestDegradeSurfacesUsageErrorsAndCancellation(t *testing.T) {
	client, err := New("127.0.0.1:1",
		WithDialTimeout(100*time.Millisecond),
		Degrade(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()

	oversized := make([]byte, 2<<20)
	if err := client.Set(ctx, "k", oversized, time.Minute); err == nil || !strings.Contains(err.Error(), "maximum") {
		t.Fatalf("oversized set was absorbed: %v", err)
	}
	longKey := strings.Repeat("k", 300)
	if _, _, err := client.Get[[]byte](ctx, longKey); err == nil || !strings.Contains(err.Error(), "maximum") {
		t.Fatalf("over-long key was absorbed: %v", err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := client.Get[[]byte](canceled, "k"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled read was absorbed: %v", err)
	}
	if err := client.Set(canceled, "k", []byte("v"), time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled write was absorbed: %v", err)
	}
}

// RefreshAhead at or beyond the TTL would put every write-back already inside
// the refresh window, recomputing forever under steady reads.
func TestFetchRejectsRefreshAheadBeyondTTL(t *testing.T) {
	client, err := New("unused")
	if err != nil {
		t.Fatal(err)
	}
	loader := func(context.Context) ([]byte, error) { return []byte("x"), nil }
	_, err = client.Fetch(context.Background(), "k", 10*time.Second, loader, RefreshAhead(30*time.Second))
	if err == nil || !strings.Contains(err.Error(), "RefreshAhead") {
		t.Fatalf("oversized refresh window was accepted: %v", err)
	}
}

// A dead backend address is a real infrastructure failure; no mock needed.
func TestDegradePolicyAgainstDeadBackend(t *testing.T) {
	var hooks atomic.Int32
	client, err := New("127.0.0.1:1",
		WithDialTimeout(100*time.Millisecond),
		Degrade(true),
		OnError(func(error) { hooks.Add(1) }),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()

	// Reads degrade into a miss.
	if _, ok, err := client.Get[[]byte](ctx, "k"); err != nil || ok {
		t.Fatalf("degraded get: ok=%v err=%v", ok, err)
	}
	if found, err := client.GetMany[[]byte](ctx, []string{"a", "b"}); err != nil || len(found) != 0 {
		t.Fatalf("degraded get many: %#v, %v", found, err)
	}
	if _, ok, err := client.Get[[]byte](ctx, "k", Touch(time.Minute)); err != nil || ok {
		t.Fatalf("degraded get with touch: ok=%v err=%v", ok, err)
	}
	if _, ok, err := client.Inspect(ctx, "k"); err != nil || ok {
		t.Fatalf("degraded inspect: ok=%v err=%v", ok, err)
	}

	// Unconditional writes give up silently.
	silent := map[string]func() error{
		"Set":        func() error { return client.Set(ctx, "k", []byte("v"), time.Minute) },
		"SetMany":    func() error { return client.SetMany(ctx, map[string][]byte{"k": []byte("v")}, time.Minute) },
		"Delete":     func() error { return client.Delete(ctx, "k") },
		"DeleteMany": func() error { return client.DeleteMany(ctx, []string{"k"}) },
		"Invalidate": func() error { return client.Invalidate(ctx, "k", time.Minute) },
		"Touch":      func() error { return client.Touch(ctx, "k", time.Minute) },
		"Append":     func() error { return client.Append(ctx, "k", []byte("v"), time.Minute) },
		"Prepend":    func() error { return client.Prepend(ctx, "k", []byte("v"), time.Minute) },
	}
	for name, call := range silent {
		if err := call(); err != nil {
			t.Errorf("degraded %s: %v", name, err)
		}
	}

	// Verbs whose answer feeds a business decision keep failing loudly.
	loud := map[string]func() error{
		"Add":     func() error { _, err := client.Add(ctx, "k", []byte("v"), time.Minute); return err },
		"Replace": func() error { _, err := client.Replace(ctx, "k", []byte("v"), time.Minute); return err },
		"Incr":    func() error { _, err := client.Incr(ctx, "k", 1, time.Minute); return err },
		"Decr":    func() error { _, err := client.Decr(ctx, "k", 1, time.Minute); return err },
		"Take":    func() error { _, err := client.Take(ctx, "k"); return err },
		"Update": func() error {
			_, err := client.Update(ctx, "k", time.Minute, func([]byte, bool) ([]byte, error) { return []byte("v"), nil })
			return err
		},
	}
	for name, call := range loud {
		if err := call(); err == nil {
			t.Errorf("degraded %s did not surface the failure", name)
		}
	}

	// Fetch degrades into a local computation.
	value, err := client.Fetch(ctx, "k", time.Minute, func(context.Context) ([]byte, error) { return []byte("local"), nil })
	if err != nil || string(value) != "local" {
		t.Fatalf("degraded fetch: %q, %v", value, err)
	}

	if hooks.Load() == 0 {
		t.Fatal("absorbed failures never reached OnError")
	}
}
