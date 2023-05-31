package raft

import (
	"time"
)

// if the peer has not acked in this duration, it's considered inactive.
const activeWindowWidth = 2 * baseElectionTimeout * time.Millisecond

type PeerTracker struct {
	nextIndex  int
	matchIndex int

	lastAck time.Time
}

func (rf *Raft) resetTrackedIndex() {

	for i, _ := range rf.peerTrackers {
		if i != rf.me {
			rf.peerTrackers[i].nextIndex = rf.log.LastLogIndex + 1 // 成为了leader，默认nextIndex都是从rf.log.LastLogIndex + 1开始
			rf.peerTrackers[i].matchIndex = 0                      //成为leader时，将其nextIndex和matchIndex置为

			DPrintf(50, "实例 %d 更新实例%d的nextIndex为: %d...", rf.me, i, rf.peerTrackers[i].nextIndex)

		}
	}
}
func (rf *Raft) quorumActive() bool {
	activePeers := 1
	for i, tracker := range rf.peerTrackers {
		if i != rf.me && time.Since(tracker.lastAck) <= activeWindowWidth {
			activePeers++
		}
	}
	return 2*activePeers > len(rf.peers)
}
