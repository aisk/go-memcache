package memcache

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Every server gets a small number of multiplexed connections, one by
// default. Commands arriving concurrently are queued on a connection, written
// to the socket in a single flush and answered in order, so N goroutines
// talking to one server cost one round trip on one socket rather than N of
// each. Nothing is retried on the caller's behalf: a command whose bytes may
// have reached the wire is reported as such through the written count.

// Request states. A request moves forward only; the transitions are
// compare-and-swap so that exactly one party settles each request.
const (
	requestQueued  int32 = iota // in a pending list, nothing on the wire
	requestWriting              // claimed by a writer: on the wire, or about to be
	requestDone                 // outcome delivered to the caller
)

// maxRetainedBuffer bounds the write buffer a connection keeps between
// flushes, so one oversized value does not pin its memory for good.
const maxRetainedBuffer = 64 * 1024

// request is one framed exchange: a payload and the rule that decides when
// its response stream is complete.
type request struct {
	payload  []byte
	stop     func(RawResponse) bool
	deadline time.Time // zero when the caller set none
	state    atomic.Int32
	conn     atomic.Pointer[muxConn] // the connection whose writer claimed it
	done     chan struct{}

	// onWire is set by the writer once the request's bytes have all been
	// written, and read by the teardown path to tell a lost request from
	// one the writer will still settle itself. Guarded by the connection's
	// mutex.
	onWire bool

	// Set by whichever party completes the request, before done is closed.
	responses []RawResponse
	written   int
	err       error
}

// complete settles the request unless the caller already gave up on it.
func (r *request) complete(responses []RawResponse, written int, err error) {
	if !r.state.CompareAndSwap(requestWriting, requestDone) && !r.state.CompareAndSwap(requestQueued, requestDone) {
		return
	}
	r.responses, r.written, r.err = responses, written, err
	close(r.done)
}

// connectionLostError reports that a connection died before a request's
// response arrived because of a failure observed on another request sharing
// it. It deliberately does not unwrap: the cause describes the other
// request's outcome, not this one's.
type connectionLostError struct{ cause error }

func (e *connectionLostError) Error() string {
	return "memcache: connection lost before the response arrived: " + e.cause.Error()
}

type serverPool struct {
	address string
	config  *config
	ctx     context.Context // bounds dialing; canceled by Client.Close

	mu     sync.Mutex
	conns  []*muxConn
	closed bool
}

// exchange executes one or more already-framed requests as a unit. stop
// decides when the unit's response stream is complete. written reports how
// many payload bytes reached the connection, which is what decides whether
// a failed side-effecting command is ambiguous.
func (p *serverPool) exchange(ctx context.Context, payload []byte, stop func(RawResponse) bool) (responses []RawResponse, written int, err error) {
	if err = ctx.Err(); err != nil {
		return nil, 0, err
	}
	if p.config.ioTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.config.ioTimeout)
		defer cancel()
	}
	r := &request{payload: payload, stop: stop, done: make(chan struct{})}
	if deadline, ok := ctx.Deadline(); ok {
		r.deadline = deadline
	}
	if err = p.enqueue(r); err != nil {
		return nil, 0, err
	}
	select {
	case <-r.done:
	case <-ctx.Done():
		err = ctx.Err()
		if r.state.CompareAndSwap(requestQueued, requestDone) {
			// Never claimed by a writer, so the wire never saw it.
			return nil, 0, err
		}
		if r.state.CompareAndSwap(requestWriting, requestDone) {
			// The response is orphaned; the reader drops it on arrival and
			// the connection stays healthy for its other users. The
			// exception is the oldest command outliving its deadline: no
			// answer for it means no answer for anything behind it, so
			// the connection is unresponsive and comes down now.
			if c := r.conn.Load(); c != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					c.expire(r)
				} else {
					c.orphan()
				}
			}
			return nil, len(payload), err
		}
		<-r.done
	}
	return r.responses, r.written, r.err
}

func (p *serverPool) enqueue(r *request) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrClosed
	}
	p.pickLocked().add(r)
	return nil
}

// pickLocked chooses the least loaded live connection, dropping idle ones
// past their idle timeout on the way. A new connection is dialed only when
// every existing one has commands outstanding and the cap allows it.
func (p *serverPool) pickLocked() *muxConn {
	now := time.Now()
	var best *muxConn
	bestLoad := 0
	kept := p.conns[:0]
	for _, c := range p.conns {
		load, idleSince, closed := c.status()
		if closed {
			continue
		}
		if load == 0 && p.config.idleTimeout > 0 && now.Sub(idleSince) > p.config.idleTimeout {
			c.retire()
			continue
		}
		kept = append(kept, c)
		if best == nil || load < bestLoad {
			best, bestLoad = c, load
		}
	}
	for i := len(kept); i < len(p.conns); i++ {
		p.conns[i] = nil
	}
	p.conns = kept
	if best == nil || (bestLoad > 0 && len(p.conns) < p.config.maxConns) {
		best = p.newConn()
		p.conns = append(p.conns, best)
	}
	return best
}

// detach forgets a dead connection and re-homes the requests it never
// wrote. Requeueing is allowed only for a connection that had served
// traffic, so a server that refuses every connection fails requests instead
// of redialing forever.
func (p *serverPool) detach(c *muxConn, pending []*request, cause error, requeue bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, candidate := range p.conns {
		if candidate == c {
			p.conns = append(p.conns[:i], p.conns[i+1:]...)
			break
		}
	}
	if p.closed {
		requeue, cause = false, ErrClosed
	}
	for _, r := range pending {
		if r.state.Load() != requestQueued {
			continue
		}
		if requeue {
			p.pickLocked().add(r)
		} else {
			r.complete(nil, 0, cause)
		}
	}
}

// close stops accepting work. Commands already on the wire finish their
// exchange; queued ones fail with ErrClosed.
func (p *serverPool) close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	conns := p.conns
	p.conns = nil
	p.mu.Unlock()
	for _, c := range conns {
		c.drain()
	}
}

// muxConn is one socket shared by many in-flight requests. A writer
// goroutine flushes whatever has queued while it was busy, and a reader
// goroutine hands responses to requests in submission order.
type muxConn struct {
	pool *serverPool
	wake chan struct{}

	mu         sync.Mutex
	pending    []*request // queued, not yet claimed by the writer
	inflight   []*request // written (or being written), awaiting responses
	conn       net.Conn   // nil until dialed
	reader     *bufio.Reader
	closed     bool
	cause      error    // why the connection was torn down
	blamed     *request // the request whose response the failure was seen on
	closing    bool     // pool closed: finish in-flight requests, then close
	served     bool     // delivered at least one response; gates requeueing
	lastActive time.Time
	buffer     []byte
}

func (p *serverPool) newConn() *muxConn {
	c := &muxConn{pool: p, wake: make(chan struct{}, 1), lastActive: time.Now()}
	go c.run()
	return c
}

func (c *muxConn) status() (load int, idleSince time.Time, closed bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pending) + len(c.inflight), c.lastActive, c.closed
}

func (c *muxConn) add(r *request) {
	c.mu.Lock()
	c.pending = append(c.pending, r)
	c.mu.Unlock()
	c.signal()
}

func (c *muxConn) signal() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *muxConn) run() {
	ctx := c.pool.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	cancel := context.CancelFunc(func() {})
	if c.pool.config.dialTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.pool.config.dialTimeout)
	}
	dialed, err := c.pool.config.dial(ctx, c.pool.config.network, c.pool.address)
	cancel()
	if err != nil {
		c.fail(err, false)
		return
	}
	if tcp, ok := dialed.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		_ = dialed.Close()
		return
	}
	c.conn = dialed
	c.reader = bufio.NewReader(dialed)
	c.mu.Unlock()
	go c.readLoop()
	c.writeLoop()
}

// writeLoop claims everything queued and writes it in one flush. Requests
// arriving during a flush accumulate and go out together in the next one,
// which is where the pipelining comes from.
func (c *muxConn) writeLoop() {
	for {
		<-c.wake
		for {
			c.mu.Lock()
			if c.closed {
				c.mu.Unlock()
				return
			}
			queued := c.pending
			c.pending = nil
			batch := queued[:0]
			for _, r := range queued {
				r.conn.Store(c)
				if r.state.CompareAndSwap(requestQueued, requestWriting) {
					batch = append(batch, r)
				}
			}
			if len(batch) == 0 {
				c.mu.Unlock()
				break
			}
			idle := len(c.inflight) == 0
			c.inflight = append(c.inflight, batch...)
			if idle {
				c.armReadDeadlineLocked()
			}
			conn := c.conn
			c.mu.Unlock()
			c.flush(conn, batch)
		}
	}
}

func (c *muxConn) flush(conn net.Conn, batch []*request) {
	buffer := c.buffer[:0]
	ends := make([]int, len(batch))
	var deadline time.Time
	for i, r := range batch {
		buffer = append(buffer, r.payload...)
		ends[i] = len(buffer)
		if !r.deadline.IsZero() && (deadline.IsZero() || r.deadline.Before(deadline)) {
			deadline = r.deadline
		}
	}
	_ = conn.SetWriteDeadline(deadline)
	written, err := writeAll(conn, buffer)
	if cap(buffer) <= maxRetainedBuffer {
		c.buffer = buffer[:0]
	} else {
		c.buffer = nil
	}
	c.mu.Lock()
	closed, cause, blamed, served := c.closed, c.cause, c.blamed, c.served
	if err == nil && !closed {
		for _, r := range batch {
			r.onWire = true
		}
		c.mu.Unlock()
		return
	}
	if !closed {
		// Only bytes that reached the wire can be lost. The unwritten tail
		// leaves the in-flight list so the teardown does not report it.
		for i, r := range batch {
			if written < ends[i] {
				c.inflight = c.inflight[:len(c.inflight)-(len(batch)-i)]
				break
			}
			r.onWire = true
		}
	}
	c.mu.Unlock()
	var requeue []*request
	for i, r := range batch {
		start := 0
		if i > 0 {
			start = ends[i-1]
		}
		switch {
		case written >= ends[i]:
			if closed {
				// The teardown ran mid-flush and left this batch to us.
				lost := error(&connectionLostError{cause: cause})
				if r == blamed {
					lost = cause
				}
				r.complete(r.responses, len(r.payload), lost)
			}
		case written > start:
			r.complete(nil, written-start, err)
		case served && r.state.CompareAndSwap(requestWriting, requestQueued):
			requeue = append(requeue, r)
		default:
			r.complete(nil, 0, err)
		}
	}
	if err != nil {
		c.fail(err, false)
	}
	c.pool.detach(nil, requeue, err, served)
}

// readLoop delivers responses to the oldest in-flight request until its
// stop rule is satisfied, then moves to the next one.
func (c *muxConn) readLoop() {
	for {
		response, err := readRawResponse(c.reader, c.pool.config.maxItemSize)
		if err != nil {
			var framed *framedResponseError
			if !errors.As(err, &framed) {
				if errors.Is(err, os.ErrDeadlineExceeded) {
					// The oldest command outlived its deadline with no
					// answer: the connection is unresponsive.
					err = context.DeadlineExceeded
				}
				c.fail(err, true)
				return
			}
		}
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return
		}
		if len(c.inflight) == 0 {
			c.mu.Unlock()
			c.fail(&ProtocolError{Message: "response without a request"}, true)
			return
		}
		head := c.inflight[0]
		head.responses = append(head.responses, response)
		if !head.stop(response) {
			c.mu.Unlock()
			continue
		}
		c.inflight[0] = nil
		c.inflight = c.inflight[1:]
		c.served = true
		c.lastActive = time.Now()
		c.armReadDeadlineLocked()
		finished := c.closing && len(c.inflight) == 0
		if finished {
			c.closed = true
			_ = c.conn.Close()
		}
		c.mu.Unlock()
		head.complete(head.responses, len(head.payload), nil)
		if finished {
			c.signal()
			return
		}
	}
}

// armReadDeadlineLocked makes the oldest live in-flight request's deadline
// the connection's read deadline. Only the oldest counts: a younger request
// that times out is simply orphaned, since the server may still be busy
// with an older one, but the oldest going unanswered means nobody behind it
// can be answered either. Orphans ahead of it are skipped because nobody
// is waiting on their deadlines any more.
func (c *muxConn) armReadDeadlineLocked() {
	if c.conn == nil {
		return
	}
	var deadline time.Time
	for _, r := range c.inflight {
		if r.state.Load() != requestDone {
			deadline = r.deadline
			break
		}
	}
	_ = c.conn.SetReadDeadline(deadline)
}

// oldestLiveLocked reports whether every in-flight request ahead of r has
// already been settled, which makes r the request the read deadline was
// armed for.
func (c *muxConn) oldestLiveLocked(r *request) bool {
	for _, candidate := range c.inflight {
		if candidate == r {
			return true
		}
		if candidate.state.Load() != requestDone {
			return false
		}
	}
	return false
}

// fail tears the connection down. The oldest in-flight request receives
// cause itself when the failure was observed while reading its response;
// every other in-flight request learns only that the connection was lost.
// Queued requests never touched the wire and move to another connection.
func (c *muxConn) fail(cause error, headSaw bool) {
	c.mu.Lock()
	c.teardownLocked(cause, headSaw)
}

// expire is a caller reporting that r's deadline passed unanswered. If r
// was the oldest live request the connection is unresponsive and comes
// down; otherwise r is orphaned like any other abandoned request.
func (c *muxConn) expire(r *request) {
	c.mu.Lock()
	if !c.closed && c.oldestLiveLocked(r) {
		c.teardownLocked(context.DeadlineExceeded, true)
		return
	}
	c.armReadDeadlineLocked()
	c.mu.Unlock()
}

// orphan is a caller abandoning a written request. The read deadline moves
// on to whichever live request is now the oldest.
func (c *muxConn) orphan() {
	c.mu.Lock()
	if !c.closed {
		c.armReadDeadlineLocked()
	}
	c.mu.Unlock()
}

// teardownLocked is fail with the mutex held; it releases the mutex.
// Requests still being written are left to the writer, which knows after
// the write which of them reached the wire.
func (c *muxConn) teardownLocked(cause error, headSaw bool) {
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.cause = cause
	if headSaw && len(c.inflight) > 0 {
		c.blamed = c.inflight[0]
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}
	inflight, pending, served := c.inflight, c.pending, c.served
	c.inflight, c.pending = nil, nil
	c.mu.Unlock()
	c.signal()
	for _, r := range inflight {
		if !r.onWire {
			continue
		}
		err := error(&connectionLostError{cause: cause})
		if r == c.blamed {
			err = cause
		}
		r.complete(r.responses, len(r.payload), err)
	}
	c.pool.detach(c, pending, cause, served)
}

// drain is the pool-close path: queued requests fail, in-flight ones are
// allowed to finish, and the socket closes once they have.
func (c *muxConn) drain() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	pending := c.pending
	c.pending = nil
	c.closing = true
	if len(c.inflight) == 0 {
		c.closed = true
		if c.conn != nil {
			_ = c.conn.Close()
		}
	}
	c.mu.Unlock()
	c.signal()
	for _, r := range pending {
		r.complete(nil, 0, ErrClosed)
	}
}

// retire closes an idle connection with nothing to settle.
func (c *muxConn) retire() {
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		if c.conn != nil {
			_ = c.conn.Close()
		}
	}
	c.mu.Unlock()
	c.signal()
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
