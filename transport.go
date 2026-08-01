package memcache

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

type pooledConn struct {
	net.Conn
	reader    *bufio.Reader
	idleSince time.Time
}

type serverPool struct {
	address string
	config  *config
	mu      sync.Mutex
	idle    []*pooledConn // LIFO: most recently released is last.
	closed  bool
}

func (p *serverPool) acquire(ctx context.Context) (*pooledConn, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrClosed
	}
	// idle is ordered by release time, so expired connections form a prefix.
	// Pruning it here keeps long-dead sockets from holding file descriptors
	// while steady traffic keeps the newest connections fresh.
	var expired []*pooledConn
	if p.config.idleTimeout > 0 && len(p.idle) != 0 {
		cutoff := time.Now().Add(-p.config.idleTimeout)
		drop := 0
		for drop < len(p.idle) && p.idle[drop].idleSince.Before(cutoff) {
			drop++
		}
		if drop > 0 {
			expired = append(expired, p.idle[:drop]...)
			kept := copy(p.idle, p.idle[drop:])
			for i := kept; i < len(p.idle); i++ {
				p.idle[i] = nil
			}
			p.idle = p.idle[:kept]
		}
	}
	// Take the most recently released connection: it is the least likely to
	// have been silently dropped by the server or an intermediary while idle.
	var conn *pooledConn
	if n := len(p.idle); n != 0 {
		conn = p.idle[n-1]
		p.idle[n-1] = nil
		p.idle = p.idle[:n-1]
	}
	p.mu.Unlock()
	for _, stale := range expired {
		_ = stale.Close()
	}
	if conn != nil {
		return conn, nil
	}
	dialCtx := ctx
	cancel := func() {}
	if p.config.dialTimeout > 0 {
		dialCtx, cancel = context.WithTimeout(ctx, p.config.dialTimeout)
	}
	defer cancel()
	dialed, err := p.config.dial(dialCtx, p.config.network, p.address)
	if err != nil {
		return nil, err
	}
	if tcp, ok := dialed.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}
	return &pooledConn{Conn: dialed, reader: bufio.NewReader(dialed)}, nil
}

func (p *serverPool) release(conn *pooledConn) {
	if err := conn.SetDeadline(time.Time{}); err != nil {
		_ = conn.Close()
		return
	}
	p.mu.Lock()
	if !p.closed && len(p.idle) < p.config.maxIdle {
		// Stamped under the lock so idle stays ordered by release time.
		conn.idleSince = time.Now()
		p.idle = append(p.idle, conn)
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()
	_ = conn.Close()
}

func (p *serverPool) discard(conn *pooledConn) { _ = conn.Close() }

func (p *serverPool) close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	idle := p.idle
	p.idle = nil
	p.mu.Unlock()
	for _, conn := range idle {
		_ = conn.Close()
	}
}

func operationDeadline(ctx context.Context, timeout time.Duration) (time.Time, bool) {
	deadline, ok := ctx.Deadline()
	if timeout > 0 {
		local := time.Now().Add(timeout)
		if !ok || local.Before(deadline) {
			return local, true
		}
	}
	return deadline, ok
}

func writeAll(writer io.Writer, data []byte) (int, error) {
	written := 0
	for written < len(data) {
		n, err := writer.Write(data[written:])
		written += n
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, io.ErrUnexpectedEOF
		}
	}
	return written, nil
}

func interruptOnCancel(ctx context.Context, conn net.Conn) func() {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = conn.SetDeadline(time.Now())
		close(done)
	})
	return func() {
		if !stop() {
			<-done
		}
	}
}

// exchange executes one or more already-framed requests. stop decides when
// the complete response stream has been received.
func (p *serverPool) exchange(ctx context.Context, payload []byte, stop func(RawResponse) bool) (responses []RawResponse, written int, err error) {
	if err = ctx.Err(); err != nil {
		return nil, 0, err
	}
	conn, err := p.acquire(ctx)
	if err != nil {
		return nil, 0, err
	}
	safe := false
	defer func() {
		if safe {
			p.release(conn)
		} else {
			p.discard(conn)
		}
	}()
	if deadline, ok := operationDeadline(ctx, p.config.ioTimeout); ok {
		if err = conn.SetDeadline(deadline); err != nil {
			return nil, 0, err
		}
	}
	defer interruptOnCancel(ctx, conn)()
	written, writeErr := writeAll(conn, payload)
	if writeErr != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			writeErr = contextErr
		}
		return nil, written, writeErr
	}
	for {
		var response RawResponse
		response, err = readRawResponse(conn.reader, p.config.maxItemSize)
		if err != nil {
			var framed *framedResponseError
			if errors.As(err, &framed) {
				responses = append(responses, response)
				if stop(response) {
					safe = true
				}
			}
			if contextErr := ctx.Err(); contextErr != nil {
				err = contextErr
			}
			return responses, written, err
		}
		responses = append(responses, response)
		if stop(response) {
			safe = true
			return responses, written, nil
		}
	}
}

type pipelineRead struct {
	responses []RawResponse
	barrier   bool
	err       error
}

// pipeline writes and reads concurrently. This prevents a pipeline with
// large value hits from deadlocking when both peers fill their send buffers.
func (p *serverPool) pipeline(ctx context.Context, payload []byte) (responses []RawResponse, written int, err error) {
	if err = ctx.Err(); err != nil {
		return nil, 0, err
	}
	conn, err := p.acquire(ctx)
	if err != nil {
		return nil, 0, err
	}
	safe := false
	defer func() {
		if safe {
			p.release(conn)
		} else {
			p.discard(conn)
		}
	}()
	if deadline, ok := operationDeadline(ctx, p.config.ioTimeout); ok {
		if err = conn.SetDeadline(deadline); err != nil {
			return nil, 0, err
		}
	}
	defer interruptOnCancel(ctx, conn)()
	readDone := make(chan pipelineRead, 1)
	go func() {
		var result pipelineRead
		for {
			response, readErr := readRawResponse(conn.reader, p.config.maxItemSize)
			if readErr != nil {
				var framed *framedResponseError
				if errors.As(readErr, &framed) {
					result.responses = append(result.responses, response)
					continue
				}
				result.err = readErr
				_ = conn.SetDeadline(time.Now()) // unblock a concurrent writer
				readDone <- result
				return
			}
			result.responses = append(result.responses, response)
			if response.Code == ResponseNoop {
				result.barrier = true
				readDone <- result
				return
			}
		}
	}()
	written, writeErr := writeAll(conn, payload)
	if writeErr != nil {
		_ = conn.SetDeadline(time.Now())
	}
	readResult := <-readDone
	if contextErr := ctx.Err(); contextErr != nil {
		return readResult.responses, written, contextErr
	}
	if writeErr != nil {
		return readResult.responses, written, writeErr
	}
	if readResult.err != nil {
		return readResult.responses, written, readResult.err
	}
	if !readResult.barrier {
		return readResult.responses, written, &ProtocolError{Message: "pipeline omitted no-op barrier"}
	}
	safe = true
	return readResult.responses, written, nil
}
