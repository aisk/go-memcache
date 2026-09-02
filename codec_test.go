package memcache

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var _ PolicyOption = WithCodec(JSONCodec{})

type profile struct {
	Name  string   `json:"name"`
	Tags  []string `json:"tags"`
	Score int      `json:"score"`
}

// upperCodec stores strings upper-cased and counts its calls, so a test can
// tell the codec was consulted.
type upperCodec struct{ marshals, unmarshals atomic.Int32 }

func (u *upperCodec) Marshal(value any) ([]byte, error) {
	u.marshals.Add(1)
	s, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("upperCodec: %T is not a string", value)
	}
	return []byte(strings.ToUpper(s)), nil
}

func (u *upperCodec) Unmarshal(data []byte, value any) error {
	u.unmarshals.Add(1)
	p, ok := value.(*string)
	if !ok {
		return fmt.Errorf("upperCodec: %T is not a *string", value)
	}
	*p = strings.ToLower(string(data))
	return nil
}

func TestCodecOptionRejectsNil(t *testing.T) {
	if _, err := New("unused", WithCodec(nil)); err == nil || !strings.Contains(err.Error(), "codec") {
		t.Fatalf("nil codec accepted: %v", err)
	}
}

func TestIntegrationTypedObjectCache(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	alice := profile{Name: "alice", Tags: []string{"a", "b"}, Score: 3}

	if _, ok, err := c.Get[profile](ctx, "p:alice"); err != nil || ok {
		t.Fatalf("miss: ok=%v err=%v", ok, err)
	}
	if err := c.Set(ctx, "p:alice", alice, time.Minute); err != nil {
		t.Fatal(err)
	}
	got, ok, err := c.Get[profile](ctx, "p:alice")
	if err != nil || !ok || got.Name != "alice" || len(got.Tags) != 2 || got.Score != 3 {
		t.Fatalf("hit: %#v ok=%v err=%v", got, ok, err)
	}
	// []byte is the identity type: the same key reads back as the raw JSON.
	raw, ok, err := c.Get[[]byte](ctx, "p:alice")
	if err != nil || !ok || !strings.HasPrefix(string(raw), `{"name":"alice"`) {
		t.Fatalf("raw read: %q ok=%v err=%v", raw, ok, err)
	}
	// A pointer type decodes into a fresh allocation.
	ptr, ok, err := c.Get[*profile](ctx, "p:alice")
	if err != nil || !ok || ptr == nil || ptr.Name != "alice" {
		t.Fatalf("pointer read: %#v ok=%v err=%v", ptr, ok, err)
	}
	// Data that does not decode into T is an error, not a miss.
	if _, ok, err := c.Get[int](ctx, "p:alice"); err == nil || ok || !strings.Contains(err.Error(), "decoding") {
		t.Fatalf("type mismatch: ok=%v err=%v", ok, err)
	}

	if won, err := c.Add(ctx, "p:bob", profile{Name: "bob"}, time.Minute); err != nil || !won {
		t.Fatalf("add = %v, %v", won, err)
	}
	if alive, err := c.Replace(ctx, "p:bob", profile{Name: "bob", Score: 1}, time.Minute); err != nil || !alive {
		t.Fatalf("replace = %v, %v", alive, err)
	}
	if err := c.SetMany(ctx, map[string]profile{"p:carol": {Name: "carol"}, "p:dave": {Name: "dave"}}, time.Minute); err != nil {
		t.Fatal(err)
	}
	found, err := c.GetMany[profile](ctx, []string{"p:alice", "p:bob", "p:carol", "p:dave", "p:nobody"})
	if err != nil || len(found) != 4 || found["p:bob"].Score != 1 || found["p:dave"].Name != "dave" {
		t.Fatalf("get many: %#v, %v", found, err)
	}
	// One undecodable value is reported without hiding the other hits.
	if err := c.Set(ctx, "p:junk", []byte("not json"), time.Minute); err != nil {
		t.Fatal(err)
	}
	found, err = c.GetMany[profile](ctx, []string{"p:alice", "p:junk"})
	if err == nil || !strings.Contains(err.Error(), "p:junk") || len(found) != 1 || found["p:alice"].Name != "alice" {
		t.Fatalf("partial decode failure: %#v, %v", found, err)
	}

	// Values whose encoding fails never reach the wire.
	if err := c.Set(ctx, "p:bad", make(chan int), time.Minute); err == nil || !strings.Contains(err.Error(), "encoding") {
		t.Fatalf("unencodable value accepted: %v", err)
	}
	if err := c.SetMany(ctx, map[string]any{"p:bad": make(chan int)}, time.Minute); err == nil || !strings.Contains(err.Error(), "p:bad") {
		t.Fatalf("unencodable SetMany value accepted: %v", err)
	}
}

func TestIntegrationTypedFetchAndUpdate(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()

	var loads atomic.Int32
	loader := func(context.Context) (profile, error) {
		loads.Add(1)
		time.Sleep(50 * time.Millisecond)
		return profile{Name: "erin", Tags: []string{"x"}}, nil
	}
	// Every waiter, winner included, receives its own decoded copy of the
	// stored form, so a mutable field is never shared across goroutines.
	const callers = 4
	results := make([]profile, callers)
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], errs[i] = c.Fetch(ctx, "p:erin", time.Minute, loader)
		}()
	}
	wg.Wait()
	for i := range callers {
		if errs[i] != nil || results[i].Name != "erin" {
			t.Fatalf("caller %d: %#v, %v", i, results[i], errs[i])
		}
		for j := range i {
			if &results[i].Tags[0] == &results[j].Tags[0] {
				t.Fatalf("callers %d and %d share the decoded slice", i, j)
			}
		}
	}
	if loads.Load() != 1 {
		t.Fatalf("loader ran %d times", loads.Load())
	}
	if got, ok, err := c.Get[profile](ctx, "p:erin"); err != nil || !ok || got.Name != "erin" {
		t.Fatalf("fetched value was not stored: %#v ok=%v err=%v", got, ok, err)
	}
	// A loader value the codec cannot encode is a Fetch error.
	if _, err := c.Fetch(ctx, "p:chan", time.Minute, func(context.Context) (chan int, error) { return make(chan int), nil }); err == nil || !strings.Contains(err.Error(), "encoding") {
		t.Fatalf("unencodable loader value: %v", err)
	}

	// Update hands fn the decoded value and stores what fn returns.
	got, err := c.Update(ctx, "p:erin", time.Minute, func(current profile, found bool) (profile, error) {
		if !found || current.Name != "erin" {
			return profile{}, fmt.Errorf("unexpected state: %#v %v", current, found)
		}
		current.Score++
		return current, nil
	})
	if err != nil || got.Score != 1 {
		t.Fatalf("update: %#v, %v", got, err)
	}
	if got, _, _ := c.Get[profile](ctx, "p:erin"); got.Score != 1 {
		t.Fatalf("update was not stored: %#v", got)
	}
	// A miss starts fn from the zero value.
	got, err = c.Update(ctx, "p:new", time.Minute, func(current profile, found bool) (profile, error) {
		if found || current.Name != "" {
			return profile{}, fmt.Errorf("unexpected state: %#v %v", current, found)
		}
		return profile{Name: "new"}, nil
	})
	if err != nil || got.Name != "new" {
		t.Fatalf("miss update: %#v, %v", got, err)
	}
	// A stored value that does not decode into T aborts without writing.
	if err := c.Set(ctx, "p:junk", []byte("not json"), time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Update(ctx, "p:junk", time.Minute, func(profile, bool) (profile, error) { return profile{Name: "x"}, nil }); err == nil || !strings.Contains(err.Error(), "decoding") {
		t.Fatalf("undecodable current value: %v", err)
	}
	if raw, _, _ := c.Get[[]byte](ctx, "p:junk"); string(raw) != "not json" {
		t.Fatalf("aborted update wrote %q", raw)
	}
}

func TestIntegrationCustomCodec(t *testing.T) {
	codec := &upperCodec{}
	c := integrationClient(t, WithCodec(codec))
	ctx := context.Background()

	if err := c.Set(ctx, "s", "hello", time.Minute); err != nil {
		t.Fatal(err)
	}
	if raw, _, err := c.Get[[]byte](ctx, "s"); err != nil || string(raw) != "HELLO" {
		t.Fatalf("stored bytes: %q, %v", raw, err)
	}
	if got, ok, err := c.Get[string](ctx, "s"); err != nil || !ok || got != "hello" {
		t.Fatalf("decoded: %q ok=%v err=%v", got, ok, err)
	}
	if codec.marshals.Load() != 1 || codec.unmarshals.Load() != 1 {
		t.Fatalf("codec calls: %d marshals, %d unmarshals", codec.marshals.Load(), codec.unmarshals.Load())
	}
	// Counter and raw bytes verbs bypass the codec entirely.
	if _, err := c.Incr(ctx, "n", 1, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := c.Append(ctx, "s", []byte("!"), time.Minute); err != nil {
		t.Fatal(err)
	}
	if taken, err := c.Take(ctx, "s"); err != nil || string(taken) != "HELLO!" {
		t.Fatalf("take: %q, %v", taken, err)
	}
	if codec.marshals.Load() != 1 || codec.unmarshals.Load() != 1 {
		t.Fatalf("codec consulted by a raw verb: %d marshals, %d unmarshals", codec.marshals.Load(), codec.unmarshals.Load())
	}
	// Codec failures surface as errors, not misses.
	if err := c.Set(ctx, "s", 42, time.Minute); err == nil || !strings.Contains(err.Error(), "not a string") {
		t.Fatalf("codec error was not surfaced: %v", err)
	}
}
