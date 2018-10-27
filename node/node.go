package node

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"go.etcd.io/etcd/etcdserver/api/snap"

	"go.etcd.io/etcd/etcdserver/api/rafthttp"
	stats "go.etcd.io/etcd/etcdserver/api/v2stats"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal"
	"go.etcd.io/etcd/wal/walpb"
)

var defaultSnapshotCount uint64 = 10000

type Node struct {
	nodeDir string
	walDir  string
	snapDir string

	raft        raft.Node
	raftStorage *raft.MemoryStorage
	wal         *wal.WAL

	id    int      // client ID for raft session
	peers []string // raft peer URLs
	join  bool     // node is joining an existing cluster

	lastIndex     uint64 // index of log at start
	confState     raftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64

	snapshotter      *snap.Snapshotter
	snapshotterReady chan *snap.Snapshotter // signals when snapshotter is ready
	snapCount        uint64

	transport *rafthttp.Transport
	stopc     chan struct{} // signals proposal channel closed
	httpstopc chan struct{} // signals http server to shutdown
	httpdonec chan struct{} // signals http server shutdown complete

	proposeC    <-chan string            // proposed messages (k,v)
	confChangeC <-chan raftpb.ConfChange // proposed cluster config changes
	commitC     chan<- *string           // entries committed to log (k,v)
	errorC      chan<- error             // errors from raft session
}

func New(id int, dir string) *Node {
	return &Node{
		id:               id,
		nodeDir:          dir,
		walDir:           filepath.Join(dir, "wal"),
		snapDir:          filepath.Join(dir, "snap"),
		snapshotterReady: make(chan *snap.Snapshotter, 1),
		snapCount:        defaultSnapshotCount,
		stopc:            make(chan struct{}),
		httpstopc:        make(chan struct{}),
		httpdonec:        make(chan struct{}),
		proposeC:         make(chan string),
		commitC:          make(chan *string),
		errorC:           make(chan error),
	}
}

func (n *Node) Open() error {
	go n.startRaft() // XXX needs to be launched via goroutine?
	return nil
}

func (n *Node) Close() error {
	return nil
}

func (n *Node) startRaft() {
	n.snapshotter = snap.New(nil, n.snapDir)
	n.snapshotterReady <- n.snapshotter // XXX what's this used by?

	oldwal := wal.Exist(n.walDir)
	n.initWAL() // XXX CHECK ERROR

	rpeers := make([]raft.Peer, len(n.peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}
	c := &raft.Config{
		ID:              uint64(n.id),
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         n.raftStorage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
	}

	if oldwal {
		n.raft = raft.RestartNode(c)
	} else {
		startPeers := rpeers
		if n.join {
			startPeers = nil
		}
		n.raft = raft.StartNode(c, startPeers)
	}

	n.transport = &rafthttp.Transport{
		Logger:      nil,
		ID:          types.ID(n.id),
		ClusterID:   0x1000, // XXX should be configurable?
		Raft:        n,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(strconv.Itoa(n.id)),
		ErrorC:      make(chan error),
	}

	n.transport.Start()
	for i := range n.peers {
		if i+1 != n.id {
			n.transport.AddPeer(types.ID(i+1), []string{n.peers[i]})
		}
	}

	go n.serveRaft()
	go n.serveChannels()

}

func (n *Node) initWAL() error { // XXX HAVE THIS RETURN bool IF WAL ALREADY EXISTED.
	if !wal.Exist(n.walDir) {
		if err := os.Mkdir(n.walDir, 0750); err != nil {
			return fmt.Errorf("failed to create WAL directory: %s", err)
		}

		w, err := wal.Create(nil, n.walDir, nil)
		if err != nil {
			return fmt.Errorf("failed to create WAL: %s", err)
		}
		w.Close()
	}

	snapshot, err := n.snapshotter.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		return fmt.Errorf("error loading snapshot: %s", err)
	}

	walSnap := walpb.Snapshot{}
	if snapshot != nil {
		walSnap.Index, walSnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}

	w, err := wal.Open(nil, n.walDir, walSnap)
	if err != nil {
		return fmt.Errorf("failed to open WAL: %s", err)
	}

	_, st, ents, err := w.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to read WAL: %s", err)
	}

	n.raftStorage = raft.NewMemoryStorage() // XXX OK for production?
	if snapshot != nil {
		n.raftStorage.ApplySnapshot(*snapshot)
	}
	n.raftStorage.SetHardState(st)

	// append to storage so raft starts at the right place in log
	n.raftStorage.Append(ents)
	// send nil once lastIndex is published so client knows commit channel is current
	if len(ents) > 0 {
		n.lastIndex = ents[len(ents)-1].Index
	} else {
		n.commitC <- nil
	}

	n.wal = w
	return nil
}

func (n *Node) serveRaft() { // XXX return errors, log fatal, different goroutine structure???
	url, err := url.Parse(n.peers[n.id-1])
	if err != nil {
		log.Fatalf("raftexample: Failed parsing URL (%v)", err)
	}

	ln, err := newStoppableListener(url.Host, n.httpstopc)
	if err != nil {
		log.Fatalf("raftexample: Failed to listen rafthttp (%v)", err)
	}

	err = (&http.Server{Handler: n.transport.Handler()}).Serve(ln)
	select {
	case <-n.httpstopc:
	default:
		log.Fatalf("raftexample: Failed to serve rafthttp (%v)", err)
	}
	close(n.httpdonec)
}

func (n *Node) serveChannels() {
	snap, err := n.raftStorage.Snapshot()
	if err != nil {
		log.Fatalf("failed to snap: %v", err)
	}
	n.confState = snap.Metadata.ConfState
	n.snapshotIndex = snap.Metadata.Index
	n.appliedIndex = snap.Metadata.Index

	defer n.wal.Close()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// send proposals over raft
	go func() {
		confChangeCount := uint64(0)

		for n.proposeC != nil && n.confChangeC != nil {
			select {
			case prop, ok := <-n.proposeC:
				if !ok {
					n.proposeC = nil
				} else {
					// blocks until accepted by raft state machine
					n.raft.Propose(context.TODO(), []byte(prop))
				}

			case cc, ok := <-n.confChangeC:
				if !ok {
					n.confChangeC = nil
				} else {
					confChangeCount++
					cc.ID = confChangeCount
					n.raft.ProposeConfChange(context.TODO(), cc)
				}
			}
		}
		// client closed channel; shutdown raft if not already
		close(n.stopc)
	}()

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			n.raft.Tick()

		// store raft entries to wal, then publish over commit channel
		case d := <-n.raft.Ready():
			n.wal.Save(d.HardState, d.Entries)
			if !raft.IsEmptySnap(d.Snapshot) {
				n.saveSnap(d.Snapshot)
				n.raftStorage.ApplySnapshot(d.Snapshot)
				n.publishSnapshot(d.Snapshot)
			}
			n.raftStorage.Append(d.Entries)
			n.transport.Send(d.Messages)
			if ok := n.publishEntries(n.entriesToApply(d.CommittedEntries)); !ok {
				n.stop()
				return
			}
			n.maybeTriggerSnapshot()
			n.raft.Advance()

		case err := <-n.transport.ErrorC:
			n.writeError(err)
			return

		case <-n.stopc:
			n.stop()
			return
		}
	}
}

func (n *Node) Process(ctx context.Context, m raftpb.Message) error {
	return n.raft.Step(ctx, m)
}
func (n *Node) IsIDRemoved(id uint64) bool                           { return false }
func (n *Node) ReportUnreachable(id uint64)                          {}
func (n *Node) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}

// stoppableListener sets TCP keep-alive timeouts on accepted
// connections and waits on stopc message
type stoppableListener struct {
	*net.TCPListener
	stopc <-chan struct{}
}

func newStoppableListener(addr string, stopc <-chan struct{}) (*stoppableListener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &stoppableListener{ln.(*net.TCPListener), stopc}, nil
}

func (ln stoppableListener) Accept() (c net.Conn, err error) {
	connc := make(chan *net.TCPConn, 1)
	errc := make(chan error, 1)
	go func() {
		tc, err := ln.AcceptTCP()
		if err != nil {
			errc <- err
			return
		}
		connc <- tc
	}()
	select {
	case <-ln.stopc:
		return nil, errors.New("server stopped")
	case err := <-errc:
		return nil, err
	case tc := <-connc:
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(3 * time.Minute)
		return tc, nil
	}
}
