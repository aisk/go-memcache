package memcache

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestPipelineIssuesVerbsOnOneConnection(t *testing.T) {
	var dials atomic.Int32
	var served atomic.Int32
	client, _ := New("pipe", WithDialer(countingDialer(&dials, serveMisses(func(string) { served.Add(1) }))))
	defer client.Close()

	p := client.Pipeline()
	user := p.Get[[]byte]("user:1")
	stored := p.Set("session:1", []byte("s"), time.Minute)
	touched := p.Touch("session:2", time.Minute)
	deleted := p.Delete("stale")
	if err := p.Exec(context.Background()); err != nil {
		t.Fatal(err)
	}
	if user.OK || user.Err != nil {
		t.Fatalf("get: %#v", user)
	}
	for name, outcome := range map[string]*Outcome{"set": stored, "touch": touched, "delete": deleted} {
		if outcome.Err != nil {
			t.Fatalf("%s: %v", name, outcome.Err)
		}
	}
	if dials.Load() != 1 || served.Load() != 4 {
		t.Fatalf("dials = %d served = %d, want 1 and 4", dials.Load(), served.Load())
	}
}

// One failing verb reports through Exec and its own result while the rest
// of the pipeline still answers.
func TestPipelineKeepsOtherAnswersOnFailure(t *testing.T) {
	client, _ := New("pipe", WithDialer(pipeDialer(serveMisses(nil))))
	defer client.Close()

	p := client.Pipeline()
	bad := p.Get[[]byte](strings.Repeat("k", 300))
	good := p.Set("k", []byte("v"), time.Minute)
	err := p.Exec(context.Background())
	if err == nil || !strings.Contains(err.Error(), "maximum") {
		t.Fatalf("exec: %v", err)
	}
	if !errors.Is(bad.Err, err) {
		t.Fatalf("failed verb: %v", bad.Err)
	}
	if good.Err != nil {
		t.Fatalf("unrelated verb: %v", good.Err)
	}
}

func TestPipelineIsReusableAfterExec(t *testing.T) {
	var served atomic.Int32
	client, _ := New("pipe", WithDialer(pipeDialer(serveMisses(func(string) { served.Add(1) }))))
	defer client.Close()

	p := client.Pipeline()
	first := p.Get[[]byte]("a")
	if err := p.Exec(context.Background()); err != nil {
		t.Fatal(err)
	}
	second := p.Get[[]byte]("b")
	if err := p.Exec(context.Background()); err != nil {
		t.Fatal(err)
	}
	if first.Err != nil || second.Err != nil || served.Load() != 2 {
		t.Fatalf("first=%v second=%v served=%d", first.Err, second.Err, served.Load())
	}
	if err := p.Exec(context.Background()); err != nil {
		t.Fatalf("empty exec: %v", err)
	}
}

func TestIntegrationPipeline(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	type user struct{ Name string }
	if err := c.Set(ctx, "user:1", user{Name: "ann"}, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := c.Set(ctx, "session:1", []byte("s"), time.Minute); err != nil {
		t.Fatal(err)
	}

	p := c.Pipeline()
	got := p.Get[user]("user:1")
	missing := p.Get[user]("user:2")
	hits := p.Incr("rate:1", 1, time.Minute)
	renewed := p.Touch("session:1", time.Hour)
	won := p.Add("job:1", []byte("1"), time.Minute)
	report := p.Fetch("report:1", time.Minute, func(context.Context) (string, error) { return "built", nil })
	cart := p.Update("cart:1", time.Minute, func(items []string, found bool) ([]string, error) {
		return append(items, "item"), nil
	})
	if err := p.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if got.Err != nil || !got.OK || got.Value.Name != "ann" {
		t.Fatalf("get: %#v", got)
	}
	if missing.Err != nil || missing.OK {
		t.Fatalf("miss: %#v", missing)
	}
	if hits.Err != nil || hits.Value != 1 {
		t.Fatalf("incr: %#v", hits)
	}
	if renewed.Err != nil {
		t.Fatalf("touch: %v", renewed.Err)
	}
	if won.Err != nil || !won.Value {
		t.Fatalf("add: %#v", won)
	}
	if report.Err != nil || report.Value != "built" {
		t.Fatalf("fetch: %#v", report)
	}
	if cart.Err != nil || len(cart.Value) != 1 || cart.Value[0] != "item" {
		t.Fatalf("update: %#v", cart)
	}
	if info, ok, err := c.Inspect(ctx, "session:1"); err != nil || !ok || info.TTL <= time.Minute {
		t.Fatalf("touch did not land: %#v ok=%v err=%v", info, ok, err)
	}
}
