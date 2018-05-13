package statelesstracker

import (
	"context"
	"sync"

	"github.com/ipfs/ipfs-cluster/api"
	"github.com/ipfs/ipfs-cluster/optracker"
	"github.com/ipfs/ipfs-cluster/pintracker/ptutil"

	rpc "github.com/hsanjuan/go-libp2p-gorpc"
	cid "github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
	peer "github.com/libp2p/go-libp2p-peer"
)

var logger = logging.Logger("pintracker")

// StatelessPinTracker uses the optracker.OperationTracker to manage
// transitioning shared ipfs-cluster state (Pins) to the local IPFS node.
type StatelessPinTracker struct {
	optracker *optracker.OperationTracker

	peerID peer.ID

	ctx    context.Context
	cancel func()

	rpcClient *rpc.Client
	rpcReady  chan struct{}

	shutdownMu sync.Mutex
	shutdown   bool
	wg         sync.WaitGroup
}

// NewStatelessPinTracker creates a new StatelessPinTracker.
func NewStatelessPinTracker(pid peer.ID) *StatelessPinTracker {
	ctx, cancel := context.WithCancel(context.Background())

	spt := &StatelessPinTracker{
		peerID:   pid,
		ctx:      ctx,
		cancel:   cancel,
		rpcReady: make(chan struct{}, 1),
	}
	return spt
}

// SetClient makes the StatelessPinTracker ready to perform RPC requests to
// other components.
func (spt *StatelessPinTracker) SetClient(c *rpc.Client) {
	spt.rpcClient = c
	spt.rpcReady <- struct{}{}
}

// Shutdown finishes the services provided by the StatelessPinTracker
// and cancels any active context.
func (spt *StatelessPinTracker) Shutdown() error {
	spt.shutdownMu.Lock()
	defer spt.shutdownMu.Unlock()

	if spt.shutdown {
		logger.Debug("already shutdown")
		return nil
	}

	logger.Info("stopping StatelessPinTracker")
	spt.cancel()
	close(spt.rpcReady)
	spt.wg.Wait()
	spt.shutdown = true
	return nil
}

// Track tells the StatelessPinTracker to start managing a Cid,
// possibly triggering Pin operations on the IPFS daemon.
func (spt *StatelessPinTracker) Track(c api.Pin) error {
	logger.Debugf("tracking %s", c.Cid)
	if ptutil.IsRemotePin(c, spt.peerID) {
		if spt.Status(c.Cid).Status == api.TrackerStatusPinned {
			spt.optracker.TrackNewOperation(
				spt.ctx,
				c.Cid,
				optracker.OperationUnpin,
			)
			spt.unpin(c)
		}
		return nil
	}

	//TODO(ajl): implement track
	return nil
}

// Untrack tells the StatelessPinTracker to stop managing a Cid.
// If the Cid is pinned locally, it will be unpinned.
func (spt *StatelessPinTracker) Untrack(c *cid.Cid) error {
	panic("not implemented")
}

// StatusAll returns information for all Cids pinned to the local IPFS node.
func (spt *StatelessPinTracker) StatusAll() []api.PinInfo {
	//NOTE(ajl): might need to extend optracker to provide GetAll function

	// get statuses from ipfs node first
	// put them into a map
	// get all inflight operations from optracker
	// put them into the map, deduplicating any already 'pinned' items with
	// their inflight operation
	pis, _ := spt.SyncAll()
	return pis
}

// Status returns information for a Cid pinned to the local IPFS node.
func (spt *StatelessPinTracker) Status(c *cid.Cid) api.PinInfo {
	// check if c has an inflight operation in optracker
	if op, ok := spt.optracker.Get(c); ok {
		// if it does return the status of the operation
		pi := api.PinInfo{
			Cid:    c,
			Peer:   spt.peerID,
			Status: op.OperationPhase2TrackerStatus(),
		}
		return pi
	}
	// else attempt to get status from ipfs node
	pi, _ := spt.Sync(c)
	return pi
}

// SyncAll verifies that the statuses of all tracked Cids (from the shared state)
// match the one reported by the IPFS daemon. If not, they will be transitioned
// to PinError or UnpinError.
//
// SyncAll returns the list of local status for all tracked Cids which
// were updated or have errors. Cids in error states can be recovered
// with Recover().
// An error is returned if we are unable to contact the IPFS daemon.
func (spt *StatelessPinTracker) SyncAll() ([]api.PinInfo, error) {
	var ipsMap map[string]api.IPFSPinStatus
	err := spt.rpcClient.Call(
		"",
		"Cluster",
		"IPFSPinLs",
		"recursive",
		&ipsMap,
	)
	if err != nil {
		logger.Error(err)
		return nil, err
	}
	var pins []api.PinInfo
	for cidstr, ips := range ipsMap {
		c, err := cid.Decode(cidstr)
		if err != nil {
			logger.Error(err)
			continue
		}
		p := api.PinInfo{
			Cid:    c,
			Peer:   spt.peerID,
			Status: api.IPFSPinStatus2TrackerStatus(ips),
		}
		pins = append(pins, p)
	}
	return pins, nil
}

// Sync verifies that the status of a Cid in shared state, matches that of
// the IPFS daemon. If not, it will be transitioned
// to PinError or UnpinError.
//
// Sync returns the updated local status for the given Cid.
// Pins in error states can be recovered with Recover().
// An error is returned if we are unable to contact
// the IPFS daemon.
func (spt *StatelessPinTracker) Sync(c *cid.Cid) (api.PinInfo, error) {
	var ips api.IPFSPinStatus
	err := spt.rpcClient.Call(
		"",
		"Cluster",
		"IPFSPinLsCid",
		api.PinCid(c).ToSerial(),
		&ips,
	)
	if err != nil {
		return api.PinInfo{}, err
	}
	pi := api.PinInfo{
		Cid:    c,
		Peer:   spt.peerID,
		Status: api.IPFSPinStatus2TrackerStatus(ips),
		//TODO(ajl): figure out a way to preserve time value, represents when the cid was pinned
	}
	return pi, nil
}

// RecoverAll attempts to recover all items tracked by this peer.
func (spt *StatelessPinTracker) RecoverAll() ([]api.PinInfo, error) {
	panic("not implemented")
}

// Recover will re-track or re-untrack a Cid in error state,
// possibly retriggering an IPFS pinning operation and returning
// only when it is done. The pinning/unpinning operation happens
// synchronously, jumping the queues.
func (spt *StatelessPinTracker) Recover(c *cid.Cid) (api.PinInfo, error) {
	panic("not implemented")
}

func (spt *StatelessPinTracker) pin(c api.Pin) error {
	logger.Debugf("issuing pin call for %s", c.Cid)

	var ctx context.Context
	opc, ok := spt.optracker.Get(c.Cid)
	if !ok {
		logger.Debug("pin operation wasn't being tracked")
		ctx = spt.ctx
	} else {
		ctx = opc.Ctx
	}

	err := spt.rpcClient.CallContext(
		ctx,
		"",
		"Cluster",
		"IPFSPin",
		c.ToSerial(),
		&struct{}{},
	)
	if err != nil {
		return err
	}

	spt.optracker.Finish(c.Cid)
	return nil
}

func (spt *StatelessPinTracker) unpin(c api.Pin) error {
	logger.Debugf("issuing unpin call for %s", c.Cid)

	var ctx context.Context
	opc, ok := spt.optracker.Get(c.Cid)
	if !ok {
		logger.Debug("pin operation wasn't being tracked")
		ctx = spt.ctx
	} else {
		ctx = opc.Ctx
	}

	err := spt.rpcClient.CallContext(
		ctx,
		"",
		"Cluster",
		"IPFSUnpin",
		c.ToSerial(),
		&struct{}{},
	)
	if err != nil {
		return err
	}

	spt.optracker.Finish(c.Cid)
	return nil
}

func (spt *StatelessPinTracker) syncStatus(c *cid.Cid, ips api.IPFSPinStatus) api.PinInfo {
	return api.PinInfo{}
}
