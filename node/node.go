package node

import (
	"fmt"
	"os"
	"path/filepath"

	"go.etcd.io/etcd/etcdserver/api/snap"

	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/wal"
	"go.etcd.io/etcd/wal/walpb"
)

type Node struct {
	nodeDir string
	walDir  string
	snapDir string

	raftStorage *raft.MemoryStorage
	wal         *wal.WAL

	lastIndex     uint64 // index of log at start
	snapshotIndex uint64
	appliedIndex  uint64

	snapshotter *snap.Snapshotter

	proposeC <-chan string  // proposed messages (k,v)
	commitC  chan<- *string // entries committed to log (k,v)
}

func New(dir string) *Node {
	return &Node{
		nodeDir: dir,
		walDir:  filepath.Join(dir, "wal"),
		snapDir: filepath.Join(dir, "snap"),
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
	return
}

func (n *Node) initWAL() error {
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
		return fmt.Errorf("failed to load WAL: %s", err)
	}

	_, st, ents, err := w.ReadAll()
	if err != nil {
		return err
	}

	n.raftStorage = raft.NewMemoryStorage()
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
