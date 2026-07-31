package memcache

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
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
	go func() { _, err := client.Get(ctx, "key"); done <- err }()
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
	go func() { done <- client.Set(ctx, "key", []byte("x"), 0) }()
	<-requestRead
	cancel()
	err := <-done
	var ambiguous *AmbiguousWriteError
	if !errors.As(err, &ambiguous) || !errors.Is(err, context.Canceled) {
		t.Fatalf("got %T %v", err, err)
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
	err := client.Set(context.Background(), "key", []byte("value"), 0)
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
	go func() { _, err := client.GetWithOptions(ctx, "key", GetOptions{Touch: &ttl}); done <- err }()
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
	_, err := client.Get(context.Background(), "one")
	var protocol *ProtocolError
	if !errors.As(err, &protocol) {
		t.Fatalf("first get: %v", err)
	}
	_, err = client.Get(context.Background(), "two")
	if !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("second get: %v", err)
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
	err := client.Set(context.Background(), "one", []byte("x"), 0)
	var ambiguous *AmbiguousWriteError
	var protocol *ProtocolError
	if errors.As(err, &ambiguous) || !errors.As(err, &protocol) {
		t.Fatalf("set error = %T %v", err, err)
	}
	_, err = client.Get(context.Background(), "two")
	if !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("second get: %v", err)
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
	results, err := client.Batch(context.Background(), []Operation{
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
	results, err := client.Batch(context.Background(), []Operation{
		SetOperation{Key: "key", Value: []byte("x"), Options: SetOptions{ReturnCAS: true}},
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
	results, err := client.Batch(context.Background(), []Operation{
		GetOperation{Key: "key", Options: GetOptions{VivifyTTL: &ttl}},
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
	results, err := client.Batch(context.Background(), []Operation{SetOperation{Key: "key", Value: []byte("x")}})
	if err != nil {
		t.Fatal(err)
	}
	var ambiguous *AmbiguousWriteError
	if !results[0].Ambiguous || !errors.As(results[0].Err, &ambiguous) {
		t.Fatalf("result: %#v", results[0])
	}
}

func TestPoolReuseAndClose(t *testing.T) {
	var dials atomic.Int32
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			reader := bufio.NewReader(server)
			for {
				if _, err := reader.ReadString('\n'); err != nil {
					return
				}
				if _, err := server.Write([]byte("EN\r\n")); err != nil {
					return
				}
			}
		}()
		return client, nil
	}
	client, _ := New("pipe", WithDialer(dial), WithMaxIdleConns(1))
	for range 2 {
		if _, err := client.Get(context.Background(), "key"); !errors.Is(err, ErrCacheMiss) {
			t.Fatal(err)
		}
	}
	if dials.Load() != 1 {
		t.Fatalf("dials = %d, want 1", dials.Load())
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Get(context.Background(), "key"); !errors.Is(err, ErrClosed) {
		t.Fatalf("after Close: %v", err)
	}
}

func TestMaxIdleZeroDisablesReuse(t *testing.T) {
	var dials atomic.Int32
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			_, _ = bufio.NewReader(server).ReadString('\n')
			_, _ = server.Write([]byte("EN\r\n"))
		}()
		return client, nil
	}
	client, _ := New("pipe", WithDialer(dial), WithMaxIdleConns(0))
	for range 2 {
		_, _ = client.Get(context.Background(), "key")
	}
	if dials.Load() != 2 {
		t.Fatalf("dials = %d, want 2", dials.Load())
	}
}
