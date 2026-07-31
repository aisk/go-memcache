package memcache

import (
	"bufio"
	"context"
	"io"
	"net"
	"sync"
	"time"
)

type pooledConn struct {
	net.Conn
	reader *bufio.Reader
}

type serverPool struct {
	address string
	config  *config
	mu      sync.Mutex
	idle    []*pooledConn // FIFO: oldest is first.
	closed  bool
}

func (p *serverPool) acquire(ctx context.Context) (*pooledConn, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrClosed
	}
	if len(p.idle) != 0 {
		conn := p.idle[0]
		copy(p.idle, p.idle[1:])
		p.idle = p.idle[:len(p.idle)-1]
		p.mu.Unlock()
		return conn, nil
	}
	p.mu.Unlock()
	dialCtx := ctx
	cancel := func() {}
	if p.config.dialTimeout > 0 {
		dialCtx, cancel = context.WithTimeout(ctx, p.config.dialTimeout)
	}
	defer cancel()
	conn, err := p.config.dial(dialCtx, p.config.network, p.address)
	if err != nil {
		return nil, err
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}
	return &pooledConn{Conn: conn, reader: bufio.NewReader(conn)}, nil
}

func (p *serverPool) release(conn *pooledConn) {
	_ = conn.SetDeadline(time.Time{})
	p.mu.Lock()
	if !p.closed && len(p.idle) < p.config.maxIdle {
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
	stopCancel := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Now()) })
	defer stopCancel()
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
