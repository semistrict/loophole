package sqlitevfs

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	nbd "github.com/Merovius/nbd"
	"github.com/semistrict/loophole"
)

// NBDVolume implements loophole.Volume by proxying Read/Write/Flush over
// an NBD connection to the daemon. It only supports the operations needed
// by VolumeVFS; Snapshot/Clone/Freeze/Refresh return errors.
type NBDVolume struct {
	conn   net.Conn
	name   string
	size   uint64
	handle atomic.Uint64

	// writeMu serializes writes to conn.
	writeMu sync.Mutex

	// pending tracks in-flight requests. Each entry has:
	// - ch: receives exactly one nbdReply when the reply arrives
	// - readLen: expected data payload length (0 for non-read commands)
	pendingMu sync.Mutex
	pending   map[uint64]*pendingReq

	done chan struct{} // closed when reader goroutine exits
	err  error         // set by reader goroutine on exit
}

type pendingReq struct {
	ch      chan nbdReply
	readLen uint32
}

type nbdReply struct {
	errno uint32
	data  []byte
}

// NBD protocol constants.
const (
	nbdReqMagic         = 0x25609513
	nbdSimpleReplyMagic = 0x67446698
	nbdCmdRead          = 0
	nbdCmdWrite         = 1
	nbdCmdDisc          = 2
	nbdCmdFlush         = 3
)

// DialNBD connects to an NBD server over Unix socket, performs the
// handshake for the named export, and returns an NBDVolume.
func DialNBD(ctx context.Context, sockPath, exportName string) (*NBDVolume, error) {
	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		return nil, fmt.Errorf("dial NBD %s: %w", sockPath, err)
	}

	cl, err := nbd.ClientHandshake(ctx, conn)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("NBD handshake: %w", err)
	}

	export, err := cl.Go(exportName)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("NBD go %q: %w", exportName, err)
	}

	// ClientHandshake wraps conn with deadline-setting context cancellation.
	// After Go() returns, the wrapper calls SetDeadline(time.Now()) which
	// would cause all subsequent I/O to timeout. Clear the deadline.
	_ = conn.SetDeadline(time.Time{})

	v := &NBDVolume{
		conn:    conn,
		name:    exportName,
		size:    export.Size,
		pending: make(map[uint64]*pendingReq),
		done:    make(chan struct{}),
	}

	go v.readReplies()
	return v, nil
}

// readReplies reads NBD simple replies from the connection and dispatches
// them to the appropriate pending request channel.
func (v *NBDVolume) readReplies() {
	defer close(v.done)
	var hdr [16]byte
	for {
		if _, err := io.ReadFull(v.conn, hdr[:]); err != nil {
			v.err = err
			v.cancelAll()
			return
		}
		magic := binary.BigEndian.Uint32(hdr[0:4])
		if magic != nbdSimpleReplyMagic {
			v.err = fmt.Errorf("invalid NBD reply magic 0x%x", magic)
			v.cancelAll()
			return
		}
		errno := binary.BigEndian.Uint32(hdr[4:8])
		handle := binary.BigEndian.Uint64(hdr[8:16])

		v.pendingMu.Lock()
		pr, ok := v.pending[handle]
		if ok {
			delete(v.pending, handle)
		}
		v.pendingMu.Unlock()

		if !ok {
			v.err = fmt.Errorf("NBD reply for unknown handle %d", handle)
			v.cancelAll()
			return
		}

		rep := nbdReply{errno: errno}
		if pr.readLen > 0 {
			rep.data = make([]byte, pr.readLen)
			if _, err := io.ReadFull(v.conn, rep.data); err != nil {
				v.err = err
				v.cancelAll()
				return
			}
		}

		pr.ch <- rep
	}
}

func (v *NBDVolume) cancelAll() {
	v.pendingMu.Lock()
	defer v.pendingMu.Unlock()
	for handle, pr := range v.pending {
		pr.ch <- nbdReply{errno: 1} // EIO
		delete(v.pending, handle)
	}
}

// sendRequest writes an NBD request and waits for the reply.
func (v *NBDVolume) sendRequest(typ uint16, offset uint64, data []byte, readLen uint32) (nbdReply, error) {
	handle := v.handle.Add(1)

	pr := &pendingReq{
		ch:      make(chan nbdReply, 1),
		readLen: readLen,
	}

	v.pendingMu.Lock()
	v.pending[handle] = pr
	v.pendingMu.Unlock()

	// Build request: magic(4) + flags(2) + type(2) + handle(8) + offset(8) + length(4) [+ data]
	length := uint32(len(data))
	if typ == nbdCmdRead {
		length = readLen
	}

	hdr := make([]byte, 28+len(data))
	binary.BigEndian.PutUint32(hdr[0:4], nbdReqMagic)
	binary.BigEndian.PutUint16(hdr[4:6], 0) // flags
	binary.BigEndian.PutUint16(hdr[6:8], typ)
	binary.BigEndian.PutUint64(hdr[8:16], handle)
	binary.BigEndian.PutUint64(hdr[16:24], offset)
	binary.BigEndian.PutUint32(hdr[24:28], length)
	copy(hdr[28:], data)

	v.writeMu.Lock()
	_, err := v.conn.Write(hdr)
	v.writeMu.Unlock()

	if err != nil {
		v.pendingMu.Lock()
		delete(v.pending, handle)
		v.pendingMu.Unlock()
		return nbdReply{}, err
	}

	rep := <-pr.ch
	if rep.errno != 0 {
		return rep, fmt.Errorf("NBD error %d", rep.errno)
	}
	return rep, nil
}

// --- loophole.Volume implementation ---

func (v *NBDVolume) Name() string       { return v.name }
func (v *NBDVolume) Size() uint64       { return v.size }
func (v *NBDVolume) ReadOnly() bool     { return false }
func (v *NBDVolume) VolumeType() string { return "" }
func (v *NBDVolume) AcquireRef() error  { return nil }

func (v *NBDVolume) Read(_ context.Context, buf []byte, offset uint64) (int, error) {
	rep, err := v.sendRequest(nbdCmdRead, offset, nil, uint32(len(buf)))
	if err != nil {
		return 0, err
	}
	copy(buf, rep.data)
	return len(buf), nil
}

func (v *NBDVolume) ReadAt(_ context.Context, offset uint64, n int) ([]byte, func(), error) {
	buf := make([]byte, n)
	rep, err := v.sendRequest(nbdCmdRead, offset, nil, uint32(n))
	if err != nil {
		return nil, nil, err
	}
	copy(buf, rep.data)
	return buf, func() {}, nil
}

func (v *NBDVolume) Write(data []byte, offset uint64) error {
	_, err := v.sendRequest(nbdCmdWrite, offset, data, 0)
	return err
}

func (v *NBDVolume) Flush() error {
	_, err := v.sendRequest(nbdCmdFlush, 0, nil, 0)
	return err
}

func (v *NBDVolume) PunchHole(_, _ uint64) error {
	return nil
}

func (v *NBDVolume) ZeroRange(_, _ uint64) error {
	return nil
}

func (v *NBDVolume) ReleaseRef() error {
	// Send disconnect and close the connection.
	v.writeMu.Lock()
	hdr := make([]byte, 28)
	binary.BigEndian.PutUint32(hdr[0:4], nbdReqMagic)
	binary.BigEndian.PutUint16(hdr[6:8], nbdCmdDisc)
	_, _ = v.conn.Write(hdr)
	v.writeMu.Unlock()
	return v.conn.Close()
}

// Operations not supported over NBD — use HTTP RPCs for these.
func (v *NBDVolume) Snapshot(_ string) error {
	return fmt.Errorf("snapshot not supported on NBD volume; use HTTP RPC")
}
func (v *NBDVolume) Clone(_ string) (loophole.Volume, error) {
	return nil, fmt.Errorf("clone not supported on NBD volume; use HTTP RPC")
}
func (v *NBDVolume) CopyFrom(_ loophole.Volume, _, _, _ uint64) (uint64, error) {
	return 0, fmt.Errorf("copyFrom not supported on NBD volume")
}
func (v *NBDVolume) Freeze() error {
	return nil
}
func (v *NBDVolume) Refresh(_ context.Context) error {
	return nil
}
