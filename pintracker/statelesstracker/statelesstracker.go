package statelesstracker

import (
	"context"
	"errors"
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
	config *Config

	optracker *optracker.OperationTracker

	peerID peer.ID

	ctx    context.Context
	cancel func()

	rpcClient *rpc.Client
	rpcReady  chan struct{}

	pinCh   chan api.Pin
	unpinCh chan api.Pin

	shutdownMu sync.Mutex
	shutdown   bool
	wg         sync.WaitGroup
}

// NewStatelessPinTracker creates a new StatelessPinTracker.
func NewStatelessPinTracker(cfg *Config, pid peer.ID) *StatelessPinTracker {
	ctx, cancel := context.WithCancel(context.Background())

	spt := &StatelessPinTracker{
		config:    cfg,
		peerID:    pid,
		ctx:       ctx,
		cancel:    cancel,
		optracker: optracker.NewOperationTracker(ctx),
		rpcReady:  make(chan struct{}, 1),
		pinCh:     make(chan api.Pin, cfg.MaxPinQueueSize),
		unpinCh:   make(chan api.Pin, cfg.MaxPinQueueSize),
	}

	for i := 0; i < spt.config.ConcurrentPins; i++ {
		go spt.pinWorker()
	}
	go spt.unpinWorker()

	return spt
}

// reads the queue and makes pins to the IPFS daemon one by one
func (spt *StatelessPinTracker) pinWorker() {
	for {
		select {
		case p := <-spt.pinCh:
			if opc, ok := spt.optracker.Get(p.Cid); ok && opc.Op == optracker.OperationPin {
				spt.optracker.UpdateOperationPhase(p.Cid, optracker.PhaseInProgress)
				spt.pin(p)
			}
		case <-spt.ctx.Done():
			return
		}
	}
}

// reads the queue and makes unpin requests to the IPFS daemon
func (spt *StatelessPinTracker) unpinWorker() {
	for {
		select {
		case p := <-spt.unpinCh:
			if opc, ok := spt.optracker.Get(p.Cid); ok && opc.Op == optracker.OperationUnpin {
				spt.optracker.UpdateOperationPhase(p.Cid, optracker.PhaseInProgress)
				spt.unpin(p)
			}
		case <-spt.ctx.Done():
			return
		}
	}
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

	if opc, ok := spt.optracker.Get(c.Cid); ok {
		if opc.Op == optracker.OperationUnpin {
			switch opc.Phase {
			case optracker.PhaseQueued:
				spt.optracker.Finish(c.Cid)
				return nil
			case optracker.PhaseInProgress:
				spt.optracker.Finish(c.Cid)
				// NOTE: this may leave the api.PinInfo in an error state
				// so a pin operation needs to be run on it (same as Recover)
			}
		}
	}

	spt.optracker.TrackNewOperation(spt.ctx, c.Cid, optracker.OperationPin)

	select {
	case spt.pinCh <- c:
	default:
		err := errors.New("pin queue is full")
		spt.optracker.Finish(c.Cid)
		logger.Error(err.Error())
		return err
	}
	return nil
}

// Untrack tells the StatelessPinTracker to stop managing a Cid.
// If the Cid is pinned locally, it will be unpinned.
func (spt *StatelessPinTracker) Untrack(c *cid.Cid) error {
	logger.Debugf("untracking %s", c)
	if opc, ok := spt.optracker.Get(c); ok {
		if opc.Op == optracker.OperationPin {
			spt.optracker.Finish(c) // cancel it

			switch opc.Phase {
			case optracker.PhaseQueued:
				return nil
			case optracker.PhaseInProgress:
				// continues below to run a full unpin
			}
		}
	}

	spt.optracker.TrackNewOperation(spt.ctx, c, optracker.OperationUnpin)

	select {
	case spt.unpinCh <- api.PinCid(c):
	default:
		err := errors.New("unpin queue is full")
		spt.optracker.Finish(c)
		logger.Error(err.Error())
		return err
	}
	return nil
}

// StatusAll returns information for all Cids pinned to the local IPFS node.
func (spt *StatelessPinTracker) StatusAll() []api.PinInfo {
	// get statuses from ipfs node first
	localpis, _ := spt.ipfsStatusAll()
	// put them into a map
	allStatuses := make(map[string]api.PinInfo)
	for _, pi := range localpis {
		allStatuses[pi.Cid.String()] = pi
	}
	// get all inflight operations from optracker
	infops := spt.optracker.GetAll()
	// put them into the map, deduplicating any already 'pinned' items with
	// their inflight operation
	for _, infop := range infops {
		allStatuses[infop.Cid.String()] = infop.ToPinInfo(spt.peerID)
	}
	// convert deduped map back to array
	var pis []api.PinInfo
	for _, pi := range allStatuses {
		pis = append(pis, pi)
	}
	return pis
}

// Status returns information for a Cid pinned to the local IPFS node.
func (spt *StatelessPinTracker) Status(c *cid.Cid) api.PinInfo {
	// check if c has an inflight operation in optracker
	if op, ok := spt.optracker.Get(c); ok {
		// if it does return the status of the operation
		return op.ToPinInfo(spt.peerID)
	}
	// else attempt to get status from ipfs node
	//TODO(ajl): log error
	pi, _ := spt.ipfsStatus(c)
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
	// no-op in stateless tracker implementation
	return spt.StatusAll(), nil
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
	// no-op in stateless tracker implementation
	return spt.Status(c), nil
}

// RecoverAll attempts to recover all items tracked by this peer.
func (spt *StatelessPinTracker) RecoverAll() ([]api.PinInfo, error) {
	return nil, nil
}

// Recover will re-track or re-untrack a Cid in error state,
// possibly retriggering an IPFS pinning operation and returning
// only when it is done. The pinning/unpinning operation happens
// synchronously, jumping the queues.
func (spt *StatelessPinTracker) Recover(c *cid.Cid) (api.PinInfo, error) {
	return api.PinInfo{}, nil
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

func (spt *StatelessPinTracker) ipfsStatus(c *cid.Cid) (api.PinInfo, error) {
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
		Status: ips.ToTrackerStatus(),
		//TODO(ajl): figure out a way to preserve time value, represents when the cid was pinned
	}
	return pi, nil
}

func (spt *StatelessPinTracker) ipfsStatusAll() ([]api.PinInfo, error) {
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
			Status: ips.ToTrackerStatus(),
		}
		pins = append(pins, p)
	}
	return pins, nil
}
