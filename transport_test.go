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
