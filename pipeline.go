package memcache

import (
	"context"
	"sync"
	"time"
)

// Pipeline queues verbs and issues them together. Every verb has the same
// name, parameters and semantics as on Client, minus the context, and hands
// back a deferred result that Exec fills in. A pipeline changes only when
// commands go out: each verb keeps its own behavior, including under
// Degrade, and Fetch and Update take part like any other verb.
//
// A Pipeline is not safe for concurrent use. Build one per unit of work,
// or reuse it after Exec.
type Pipeline struct {
	client *Client
	queued []func(context.Context) error
}

// Pipeline starts an empty pipeline on this client.
func (c *Client) Pipeline() *Pipeline { return &Pipeline{client: c} }

// Result is the deferred return of a pipelined verb that answers with a
// value and an error. Exec fills the fields in; before that they are zero.
type Result[T any] struct {
	Value T
	Err   error
}

// Lookup is the deferred return of a pipelined Get or Inspect. OK reports
// presence exactly as the verb's second return value does, so a miss is
// OK false with a nil Err.
type Lookup[T any] struct {
	Value T
	OK    bool
	Err   error
}

// Outcome is the deferred return of a pipelined verb that answers only with
// an error.
type Outcome struct {
	Err error
}

func (p *Pipeline) add(run func(context.Context) error) {
	p.queued = append(p.queued, run)
}

// Exec issues every verb queued since the last Exec at once and waits for
// all of them. Concurrent commands to one server share its connection, so
// they go out together and are answered in one round trip per server.
// Exec returns the first error in queue order; every deferred result also
// carries its own, so one failing command does not hide the others'
// answers. A miss is not an error here either.
func (p *Pipeline) Exec(ctx context.Context) error {
	queued := p.queued
	p.queued = nil
	errs := make([]error, len(queued))
	var wg sync.WaitGroup
	for i, run := range queued {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = run(ctx)
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// Get queues a read; see Client.Get.
func (p *Pipeline) Get[T any](key string, options ...GetOption) *Lookup[T] {
	r := &Lookup[T]{}
	p.add(func(ctx context.Context) error {
		r.Value, r.OK, r.Err = p.client.Get[T](ctx, key, options...)
		return r.Err
	})
	return r
}

// Inspect queues a metadata read; see Client.Inspect.
func (p *Pipeline) Inspect(key string) *Lookup[ItemInfo] {
	r := &Lookup[ItemInfo]{}
	p.add(func(ctx context.Context) error {
		r.Value, r.OK, r.Err = p.client.Inspect(ctx, key)
		return r.Err
	})
	return r
}

// Set queues an unconditional store; see Client.Set.
func (p *Pipeline) Set[T any](key string, value T, ttl time.Duration) *Outcome {
	r := &Outcome{}
	p.add(func(ctx context.Context) error {
		r.Err = p.client.Set(ctx, key, value, ttl)
		return r.Err
	})
	return r
}

// Add queues a store that applies only when the key is absent; see
// Client.Add. The result's Value reports whether this caller won.
func (p *Pipeline) Add[T any](key string, value T, ttl time.Duration) *Result[bool] {
	r := &Result[bool]{}
	p.add(func(ctx context.Context) error {
		r.Value, r.Err = p.client.Add(ctx, key, value, ttl)
		return r.Err
	})
	return r
}

// Replace queues a store that applies only when the key exists; see
// Client.Replace. The result's Value reports whether it did.
func (p *Pipeline) Replace[T any](key string, value T, ttl time.Duration) *Result[bool] {
	r := &Result[bool]{}
	p.add(func(ctx context.Context) error {
		r.Value, r.Err = p.client.Replace(ctx, key, value, ttl)
		return r.Err
	})
	return r
}

// Touch queues a TTL extension; see Client.Touch.
func (p *Pipeline) Touch(key string, ttl time.Duration) *Outcome {
	r := &Outcome{}
	p.add(func(ctx context.Context) error {
		r.Err = p.client.Touch(ctx, key, ttl)
		return r.Err
	})
	return r
}

// Delete queues a removal; see Client.Delete.
func (p *Pipeline) Delete(key string) *Outcome {
	r := &Outcome{}
	p.add(func(ctx context.Context) error {
		r.Err = p.client.Delete(ctx, key)
		return r.Err
	})
	return r
}

// Invalidate queues a soft invalidation; see Client.Invalidate.
func (p *Pipeline) Invalidate(key string, grace time.Duration) *Outcome {
	r := &Outcome{}
	p.add(func(ctx context.Context) error {
		r.Err = p.client.Invalidate(ctx, key, grace)
		return r.Err
	})
	return r
}

// Fetch queues a get-or-compute; see Client.Fetch.
func (p *Pipeline) Fetch[T any](key string, ttl time.Duration, loader func(context.Context) (T, error), options ...FetchOption) *Result[T] {
	r := &Result[T]{}
	p.add(func(ctx context.Context) error {
		r.Value, r.Err = p.client.Fetch(ctx, key, ttl, loader, options...)
		return r.Err
	})
	return r
}

// Update queues an atomic transformation; see Client.Update.
func (p *Pipeline) Update[T any](key string, ttl time.Duration, fn func(current T, found bool) (T, error)) *Result[T] {
	r := &Result[T]{}
	p.add(func(ctx context.Context) error {
		r.Value, r.Err = p.client.Update(ctx, key, ttl, fn)
		return r.Err
	})
	return r
}

// Incr queues a counter increment; see Client.Incr.
func (p *Pipeline) Incr(key string, delta uint64, ttl time.Duration) *Result[uint64] {
	r := &Result[uint64]{}
	p.add(func(ctx context.Context) error {
		r.Value, r.Err = p.client.Incr(ctx, key, delta, ttl)
		return r.Err
	})
	return r
}

// Decr queues a counter decrement; see Client.Decr.
func (p *Pipeline) Decr(key string, delta uint64, ttl time.Duration) *Result[uint64] {
	r := &Result[uint64]{}
	p.add(func(ctx context.Context) error {
		r.Value, r.Err = p.client.Decr(ctx, key, delta, ttl)
		return r.Err
	})
	return r
}

// Append queues a raw bytes append; see Client.Append.
func (p *Pipeline) Append(key string, fragment []byte, ttl time.Duration) *Outcome {
	r := &Outcome{}
	p.add(func(ctx context.Context) error {
		r.Err = p.client.Append(ctx, key, fragment, ttl)
		return r.Err
	})
	return r
}

// Prepend queues a raw bytes prepend; see Client.Prepend.
func (p *Pipeline) Prepend(key string, fragment []byte, ttl time.Duration) *Outcome {
	r := &Outcome{}
	p.add(func(ctx context.Context) error {
		r.Err = p.client.Prepend(ctx, key, fragment, ttl)
		return r.Err
	})
	return r
}

// Take queues an atomic read-and-delete; see Client.Take.
func (p *Pipeline) Take(key string) *Result[[]byte] {
	r := &Result[[]byte]{}
	p.add(func(ctx context.Context) error {
		r.Value, r.Err = p.client.Take(ctx, key)
		return r.Err
	})
	return r
}
