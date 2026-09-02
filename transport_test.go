package memcache

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func pipeDialer(handler func(net.Conn)) DialContextFunc {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		client, server := net.Pipe()
		go handler(server)
		return client, nil
	}
}

// serveMisses answers every mg with a miss and every other command with HD
// or MN, consuming ms values so the stream stays framed. hold, when not
// nil, is consulted before each answer and may block it; reading continues
// meanwhile, as a real socket's receive buffer would.
func serveMisses(hold func(line string)) func(net.Conn) {
	return func(conn net.Conn) {
		defer conn.Close()
		lines := make(chan string, 1024)
		go func() {
			defer close(lines)
			reader := bufio.NewReader(conn)
			for {
				line, err := reader.ReadString('\n')
				if err != nil {
					return
				}
				fields := strings.Fields(line)
				if len(fields) == 0 {
					return
				}
				if fields[0] == "ms" {
					var size int
					for _, r := range fields[2] {
						size = size*10 + int(r-'0')
					}
					if _, err := io.CopyN(io.Discard, reader, int64(size)+2); err != nil {
						return
					}
				}
				lines <- line
			}
		}()
		for line := range lines {
			if hold != nil {
				hold(line)
			}
			reply := "HD\r\n"
			switch strings.Fields(line)[0] {
			case "mg":
				reply = "EN\r\n"
			case "mn":
				reply = "MN\r\n"
			}
			if _, err := conn.Write([]byte(reply)); err != nil {
				return
			}
		}
	}
}

func countingDialer(dials *atomic.Int32, handler func(net.Conn)) DialContextFunc {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		client, server := net.Pipe()
		go handler(server)
		return client, nil
	}
}

// load reports the commands queued or in flight across a server's live
// connections, and how many connections there are.
func (p *serverPool) load() (conns, load int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		n, _, closed := c.status()
		if closed {
			continue
		}
		conns++
		load += n
	}
	return conns, load
}

func TestConcurrentCommandsShareOneConnection(t *testing.T) {
	var dials atomic.Int32
	client, _ := New("pipe", WithDialer(countingDialer(&dials, serveMisses(nil))))
	defer client.Close()
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, ok, err := client.Get[[]byte](context.Background(), "key")
			if ok {
				err = errors.New("unexpected hit")
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if dials.Load() != 1 {
		t.Fatalf("dials = %d, want 1", dials.Load())
	}
}

// gatedConn blocks its first write until released and records every write,
// so a test can pile commands up behind the first flush and see them go out
// together in the second.
type gatedConn struct {
	net.Conn
	release chan struct{}
	mu      sync.Mutex
	writes  [][]byte
}

func (c *gatedConn) Write(data []byte) (int, error) {
	c.mu.Lock()
	first := len(c.writes) == 0
	c.writes = append(c.writes, bytes.Clone(data))
	c.mu.Unlock()
	if first {
		<-c.release
	}
	return c.Conn.Write(data)
}

func TestQueuedCommandsGoOutInOneFlush(t *testing.T) {
	gate := &gatedConn{release: make(chan struct{})}
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		client, server := net.Pipe()
		go serveMisses(nil)(server)
		gate.Conn = client
		return gate, nil
	}
	client, _ := New("pipe", WithDialer(dial), WithTimeout(0))
	defer client.Close()
	var wg sync.WaitGroup
	get := func() {
		defer wg.Done()
		if _, ok, err := client.Get[[]byte](context.Background(), "key"); err != nil || ok {
			t.Errorf("ok=%v err=%v", ok, err)
		}
	}
	wg.Add(1)
	go get()
	waitFor(t, func() bool {
		gate.mu.Lock()
		defer gate.mu.Unlock()
		return len(gate.writes) == 1
	}, "first flush to start")
	const queued = 8
	for range queued {
		wg.Add(1)
		go get()
	}
	waitFor(t, func() bool { _, load := client.servers[0].load(); return load == queued+1 }, "commands to queue")
	close(gate.release)
	wg.Wait()
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if len(gate.writes) != 2 {
		t.Fatalf("writes = %d, want 2", len(gate.writes))
	}
	if n := bytes.Count(gate.writes[1], []byte("mg ")); n != queued {
		t.Fatalf("second flush carried %d commands, want %d", n, queued)
	}
}

func TestCancellationInterruptsRead(t *testing.T) {
	requestRead := make(chan struct{})
	dial := pipeDialer(func(conn net.Conn) {
		defer conn.Close()
		_, _ = bufio.NewReader(conn).ReadString('\n')
		close(requestRead)
		_, _ = io.Copy(io.Discard, conn)
	})
	client, err := New("pipe", WithDialer(dial), WithTimeout(0))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, _, err := client.Get[[]byte](ctx, "key"); done <- err }()
	<-requestRead
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled read did not return")
	}
}

func TestCanceledMutationIsAmbiguous(t *testing.T) {
	requestRead := make(chan struct{})
	dial := pipeDialer(func(conn net.Conn) {
		defer conn.Close()
		reader := bufio.NewReader(conn)
		_, _ = reader.ReadString('\n')
		buffer := make([]byte, 3)
		_, _ = io.ReadFull(reader, buffer)
		close(requestRead)
		_, _ = io.Copy(io.Discard, conn)
	})
	client, _ := New("pipe", WithDialer(dial), WithTimeout(0))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- client.Set(ctx, "key", []byte("x"), Forever) }()
	<-requestRead
	cancel()
	err := <-done
	var ambiguous *AmbiguousWriteError
	if !errors.As(err, &ambiguous) || !errors.Is(err, context.Canceled) {
		t.Fatalf("got %T %v", err, err)
	}
}

// A canceled command is orphaned, not torn down: its late response is
// dropped and the connection keeps serving the commands queued behind it.
func TestCanceledCommandKeepsConnection(t *testing.T) {
	var dials atomic.Int32
	slowRead := make(chan struct{})
	release := make(chan struct{})
	hold := func(line string) {
		if strings.HasPrefix(line, "mg slow") {
			close(slowRead)
			<-release
		}
	}
	client, _ := New("pipe", WithDialer(countingDialer(&dials, serveMisses(hold))), WithTimeout(0))
	defer client.Close()
	ctx, cancel := context.WithCancel(context.Background())
	slow := make(chan error, 1)
	go func() { _, _, err := client.Get[[]byte](ctx, "slow"); slow <- err }()
	<-slowRead
	fast := make(chan error, 1)
	go func() {
		_, ok, err := client.Get[[]byte](context.Background(), "fast")
		if ok {
			err = errors.New("unexpected hit")
		}
		fast <- err
	}()
	waitFor(t, func() bool { _, load := client.servers[0].load(); return load == 2 }, "second command to queue")
	cancel()
	if err := <-slow; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled get: %v", err)
	}
	close(release)
	if err := <-fast; err != nil {
		t.Fatalf("queued get: %v", err)
	}
	if dials.Load() != 1 {
		t.Fatalf("dials = %d, want 1", dials.Load())
	}
}

// The oldest command going unanswered past its deadline means the
// connection is unresponsive: it is closed and replaced.
func TestOldestCommandTimeoutClosesConnection(t *testing.T) {
	var dials atomic.Int32
	var hung atomic.Int32
	hold := func(string) {
		if hung.Add(1) == 1 {
			select {}
		}
	}
	client, _ := New("pipe", WithDialer(countingDialer(&dials, serveMisses(hold))), WithTimeout(50*time.Millisecond))
	defer client.Close()
	err := client.Set(context.Background(), "key", []byte("x"), Forever)
	var ambiguous *AmbiguousWriteError
	if !errors.As(err, &ambiguous) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timed out set: %T %v", err, err)
	}
	if _, ok, err := client.Get[[]byte](context.Background(), "key"); err != nil || ok {
		t.Fatalf("get after timeout: ok=%v err=%v", ok, err)
	}
	if dials.Load() != 2 {
		t.Fatalf("dials = %d, want 2", dials.Load())
	}
}

// A younger command timing out behind a slow one is orphaned; only the
// oldest command's deadline speaks for the connection.
func TestYoungerCommandTimeoutIsOrphaned(t *testing.T) {
	var dials atomic.Int32
	slowRead := make(chan struct{})
	release := make(chan struct{})
	hold := func(line string) {
		if strings.HasPrefix(line, "mg slow") {
			close(slowRead)
			<-release
		}
	}
	client, _ := New("pipe", WithDialer(countingDialer(&dials, serveMisses(hold))), WithTimeout(0))
	defer client.Close()
	slow := make(chan error, 1)
	go func() { _, _, err := client.Get[[]byte](context.Background(), "slow"); slow <- err }()
	<-slowRead
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, _, err := client.Get[[]byte](ctx, "young"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("young get: %v", err)
	}
	close(release)
	if err := <-slow; err != nil {
		t.Fatalf("slow get: %v", err)
	}
	if dials.Load() != 1 {
		t.Fatalf("dials = %d, want 1", dials.Load())
	}
}

func TestMaxConnsOpensOnlyWhileBusy(t *testing.T) {
	var dials atomic.Int32
	release := make(chan struct{})
	hold := func(string) { <-release }
	client, _ := New("pipe", WithDialer(countingDialer(&dials, serveMisses(hold))), WithTimeout(0), WithMaxConns(2))
	defer client.Close()
	var wg sync.WaitGroup
	start := func() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, ok, err := client.Get[[]byte](context.Background(), "key"); err != nil || ok {
				t.Errorf("ok=%v err=%v", ok, err)
			}
		}()
	}
	start()
	waitFor(t, func() bool { return dials.Load() == 1 }, "first dial")
	waitFor(t, func() bool { _, load := client.servers[0].load(); return load == 1 }, "first command in flight")
	start()
	waitFor(t, func() bool { return dials.Load() == 2 }, "second dial")
	start()
	waitFor(t, func() bool { _, load := client.servers[0].load(); return load == 3 }, "third command to queue")
	if conns, _ := client.servers[0].load(); conns != 2 || dials.Load() != 2 {
		t.Fatalf("conns = %d dials = %d, want 2 and 2", conns, dials.Load())
	}
	close(release)
	wg.Wait()
}

func TestServerClosedIdleConnectionIsReplaced(t *testing.T) {
	var dials atomic.Int32
	handler := func(conn net.Conn) {
		defer conn.Close()
		_, _ = bufio.NewReader(conn).ReadString('\n')
		_, _ = conn.Write([]byte("EN\r\n"))
	}
	client, _ := New("pipe", WithDialer(countingDialer(&dials, handler)))
	defer client.Close()
	if _, ok, err := client.Get[[]byte](context.Background(), "key"); err != nil || ok {
		t.Fatalf("first get: ok=%v err=%v", ok, err)
	}
	waitFor(t, func() bool { conns, _ := client.servers[0].load(); return conns == 0 }, "closed connection to be dropped")
	if _, ok, err := client.Get[[]byte](context.Background(), "key"); err != nil || ok {
		t.Fatalf("second get: ok=%v err=%v", ok, err)
	}
	if dials.Load() != 2 {
		t.Fatalf("dials = %d, want 2", dials.Load())
	}
}

// A write that fails before touching the wire moves the command to a fresh
// connection instead of failing it, as long as the old connection had
// proven itself.
type flakyConn struct {
	net.Conn
	writes *atomic.Int32
}

func (c *flakyConn) Write(data []byte) (int, error) {
	if c.writes.Add(1) == 2 {
		return 0, errors.New("write failed")
	}
	return c.Conn.Write(data)
}

func TestUnwrittenCommandMovesToFreshConnection(t *testing.T) {
	var dials, writes atomic.Int32
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		client, server := net.Pipe()
		go serveMisses(nil)(server)
		return &flakyConn{Conn: client, writes: &writes}, nil
	}
	client, _ := New("pipe", WithDialer(dial))
	defer client.Close()
	for range 2 {
		if _, ok, err := client.Get[[]byte](context.Background(), "key"); err != nil || ok {
			t.Fatalf("ok=%v err=%v", ok, err)
		}
	}
	if dials.Load() != 2 {
		t.Fatalf("dials = %d, want 2", dials.Load())
	}
}

func TestDialFailureFailsQueuedCommands(t *testing.T) {
	var dials atomic.Int32
	dialErr := errors.New("refused")
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		time.Sleep(20 * time.Millisecond)
		return nil, dialErr
	}
	client, _ := New("pipe", WithDialer(dial))
	defer client.Close()
	var wg sync.WaitGroup
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := client.Set(context.Background(), "key", []byte("x"), Forever)
			var ambiguous *AmbiguousWriteError
			if !errors.Is(err, dialErr) || errors.As(err, &ambiguous) {
				t.Errorf("got %T %v", err, err)
			}
		}()
	}
	wg.Wait()
	if dials.Load() != 1 {
		t.Fatalf("dials = %d, want 1", dials.Load())
	}
}

type partialWriteConn struct {
	net.Conn
	limit int
}

func (c *partialWriteConn) Write(data []byte) (int, error) {
	if len(data) > c.limit {
		data = data[:c.limit]
	}
	n, _ := c.Conn.Write(data)
	return n, io.ErrUnexpectedEOF
}

func TestPartialCommandWriteIsNotAmbiguous(t *testing.T) {
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		client, server := net.Pipe()
		go func() { defer server.Close(); buffer := make([]byte, 1); _, _ = io.ReadFull(server, buffer) }()
		return &partialWriteConn{Conn: client, limit: 1}, nil
	}
	client, _ := New("pipe", WithDialer(dial))
	err := client.Set(context.Background(), "key", []byte("value"), Forever)
	var ambiguous *AmbiguousWriteError
	if err == nil || errors.As(err, &ambiguous) {
		t.Fatalf("got %T %v", err, err)
	}
}

func TestCanceledMutatingGetIsAmbiguous(t *testing.T) {
	requestRead := make(chan struct{})
	dial := pipeDialer(func(conn net.Conn) {
		defer conn.Close()
		_, _ = bufio.NewReader(conn).ReadString('\n')
		close(requestRead)
		_, _ = io.Copy(io.Discard, conn)
	})
	client, _ := New("pipe", WithDialer(dial), WithTimeout(0))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	ttl := Expiration(30)
	go func() { _, err := client.Meta().Get(ctx, "key", MetaGetOptions{Touch: &ttl}); done <- err }()
	<-requestRead
	cancel()
	err := <-done
	var ambiguous *AmbiguousWriteError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("got %T %v", err, err)
	}
}

func TestFramingFailureDiscardsConnection(t *testing.T) {
	var dials atomic.Int32
	dial := pipeDialer(func(conn net.Conn) {
		defer conn.Close()
		count := dials.Add(1)
		_, _ = bufio.NewReader(conn).ReadString('\n')
		if count == 1 {
			_, _ = conn.Write([]byte("HD\n"))
			return
		}
		_, _ = conn.Write([]byte("EN\r\n"))
	})
	client, _ := New("pipe", WithDialer(dial))
	_, _, err := client.Get[[]byte](context.Background(), "one")
	var protocol *ProtocolError
	if !errors.As(err, &protocol) {
		t.Fatalf("first get: %v", err)
	}
	_, ok, err := client.Get[[]byte](context.Background(), "two")
	if err != nil || ok {
		t.Fatalf("second get: ok=%v err=%v", ok, err)
	}
	if dials.Load() != 2 {
		t.Fatalf("dials = %d, want 2", dials.Load())
	}
}

func TestFramedFlagErrorIsKnownAndConnectionReusable(t *testing.T) {
	var dials atomic.Int32
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			reader := bufio.NewReader(server)
			_, _ = reader.ReadString('\n')
			_, _ = io.CopyN(io.Discard, reader, 3) // one-byte value plus CRLF
			_, _ = server.Write([]byte("HD cnope\r\n"))
			_, _ = reader.ReadString('\n')
			_, _ = server.Write([]byte("EN\r\n"))
		}()
		return client, nil
	}
	client, _ := New("pipe", WithDialer(dial))
	err := client.Set(context.Background(), "one", []byte("x"), Forever)
	var ambiguous *AmbiguousWriteError
	var protocol *ProtocolError
	if errors.As(err, &ambiguous) || !errors.As(err, &protocol) {
		t.Fatalf("set error = %T %v", err, err)
	}
	_, ok, err := client.Get[[]byte](context.Background(), "two")
	if err != nil || ok {
		t.Fatalf("second get: ok=%v err=%v", ok, err)
	}
	if dials.Load() != 1 {
		t.Fatalf("dials = %d, want connection reuse", dials.Load())
	}
}

func TestPipelineReadsWhileWriting(t *testing.T) {
	large := make([]byte, 256*1024)
	dial := pipeDialer(func(conn net.Conn) {
		defer conn.Close()
		reader := bufio.NewReader(conn)
		_, _ = reader.ReadString('\n') // mg
		_, _ = conn.Write([]byte("VA 3 O0\r\nhit\r\n"))
		line, _ := reader.ReadString('\n') // ms
		if line == "" {
			return
		}
		_, _ = io.CopyN(io.Discard, reader, int64(len(large)+2))
		_, _ = reader.ReadString('\n') // mn
		_, _ = conn.Write([]byte("MN\r\n"))
	})
	client, _ := New("pipe", WithDialer(dial), WithTimeout(time.Second))
	results, err := client.Meta().Batch(context.Background(), []Operation{
		GetOperation{Key: "hit"},
		SetOperation{Key: "large", Value: large},
	})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Err != nil || results[0].Get == nil || string(results[0].Get.Value) != "hit" {
		t.Fatalf("get result: %#v", results[0])
	}
	if results[1].Err != nil || results[1].Mutation == nil || !results[1].Mutation.Applied() {
		t.Fatalf("set result: %#v", results[1])
	}
}

func TestMissingRequiredBatchResponseIsAmbiguous(t *testing.T) {
	dial := pipeDialer(func(conn net.Conn) {
		defer conn.Close()
		reader := bufio.NewReader(conn)
		_, _ = reader.ReadString('\n')
		_, _ = io.CopyN(io.Discard, reader, 3)
		_, _ = reader.ReadString('\n')
		_, _ = conn.Write([]byte("MN\r\n"))
	})
	client, _ := New("pipe", WithDialer(dial))
	results, err := client.Meta().Batch(context.Background(), []Operation{
		SetOperation{Key: "key", Value: []byte("x"), Options: MetaSetOptions{ReturnCAS: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var ambiguous *AmbiguousWriteError
	if !results[0].Ambiguous || !errors.As(results[0].Err, &ambiguous) {
		t.Fatalf("result: %#v", results[0])
	}
}

func TestMissingVivifyResponseIsAmbiguous(t *testing.T) {
	dial := pipeDialer(func(conn net.Conn) {
		defer conn.Close()
		reader := bufio.NewReader(conn)
		_, _ = reader.ReadString('\n')
		_, _ = reader.ReadString('\n')
		_, _ = conn.Write([]byte("MN\r\n"))
	})
	client, _ := New("pipe", WithDialer(dial))
	ttl := Expiration(30)
	results, err := client.Meta().Batch(context.Background(), []Operation{
		GetOperation{Key: "key", Options: MetaGetOptions{VivifyTTL: &ttl}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var ambiguous *AmbiguousWriteError
	if !results[0].Ambiguous || !errors.As(results[0].Err, &ambiguous) {
		t.Fatalf("result: %#v", results[0])
	}
}

func TestDuplicateOpaqueMutationIsAmbiguous(t *testing.T) {
	dial := pipeDialer(func(conn net.Conn) {
		defer conn.Close()
		reader := bufio.NewReader(conn)
		_, _ = reader.ReadString('\n')
		_, _ = io.CopyN(io.Discard, reader, 3)
		_, _ = reader.ReadString('\n')
		_, _ = conn.Write([]byte("HD O0\r\nHD O0\r\nMN\r\n"))
	})
	client, _ := New("pipe", WithDialer(dial))
	results, err := client.Meta().Batch(context.Background(), []Operation{SetOperation{Key: "key", Value: []byte("x")}})
	if err != nil {
		t.Fatal(err)
	}
	var ambiguous *AmbiguousWriteError
	if !results[0].Ambiguous || !errors.As(results[0].Err, &ambiguous) {
		t.Fatalf("result: %#v", results[0])
	}
}

func TestConnectionReuseAndClose(t *testing.T) {
	var dials atomic.Int32
	client, _ := New("pipe", WithDialer(countingDialer(&dials, serveMisses(nil))))
	for range 2 {
		if _, ok, err := client.Get[[]byte](context.Background(), "key"); err != nil || ok {
			t.Fatalf("ok=%v err=%v", ok, err)
		}
	}
	if dials.Load() != 1 {
		t.Fatalf("dials = %d, want 1", dials.Load())
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := client.Get[[]byte](context.Background(), "key"); !errors.Is(err, ErrClosed) {
		t.Fatalf("after Close: %v", err)
	}
}

func TestCloseLetsInFlightCommandsFinish(t *testing.T) {
	release := make(chan struct{})
	read := make(chan struct{})
	hold := func(string) { close(read); <-release }
	client, _ := New("pipe", WithDialer(pipeDialer(serveMisses(hold))), WithTimeout(0))
	done := make(chan error, 1)
	go func() {
		_, ok, err := client.Get[[]byte](context.Background(), "key")
		if ok {
			err = errors.New("unexpected hit")
		}
		done <- err
	}()
	<-read
	_ = client.Close()
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("in-flight get after Close: %v", err)
	}
}

func TestIdleTimeoutRedials(t *testing.T) {
	var dials atomic.Int32
	client, _ := New("pipe", WithDialer(countingDialer(&dials, serveMisses(nil))), WithIdleTimeout(10*time.Millisecond))
	defer client.Close()
	get := func(want int32) {
		t.Helper()
		if _, ok, err := client.Get[[]byte](context.Background(), "key"); err != nil || ok {
			t.Fatalf("ok=%v err=%v", ok, err)
		}
		if dials.Load() != want {
			t.Fatalf("dials = %d, want %d", dials.Load(), want)
		}
	}
	get(1)
	get(1) // immediate reuse
	time.Sleep(30 * time.Millisecond)
	get(2) // idle connection expired
}
