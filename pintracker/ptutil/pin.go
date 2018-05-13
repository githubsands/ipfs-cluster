package ptutil

import (
	"github.com/ipfs/ipfs-cluster/api"

	logging "github.com/ipfs/go-log"
	peer "github.com/libp2p/go-libp2p-peer"
)

var logger = logging.Logger("pintrackerutil")

// IsRemotePin determines whether a Pin's ReplicationFactor has
// been met, so as to either pin or unpin it from the peer.
func IsRemotePin(c api.Pin, pid peer.ID) bool {
	if c.ReplicationFactorMax < 0 {
		return false
	}
	if c.ReplicationFactorMax == 0 {
		logger.Errorf("Pin with replication factor 0! %+v", c)
	}

	for _, p := range c.Allocations {
		if p == pid {
			return false
		}
	}
	return true
}
