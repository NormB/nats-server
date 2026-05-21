// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// Setup:
//   - one hub and one spoke, the spoke solicits a leaf node connection up to the hub.
//   - one client on the spoke with 10 inbox subscriptions.
//   - one client on the hub with 10 inbox subscriptions.
//
// Questions answered by this test:
//   - How many subscriptions are live on the spoke, and how many on the hub?
//   - Does the subject interest graph propagate across the leaf connection?
//   - Do both servers end up with 10 local subs and 10 remote-registered subs?
func TestLeafNodeInboxInterestPropagation(t *testing.T) {
	// Hub: accepts leaf node connections.
	lo := DefaultTestOptions
	lo.Port = -1
	lo.LeafNode.Host = lo.Host
	lo.LeafNode.Port = -1
	lo.NoSystemAccount = true
	hub := RunServer(&lo)
	defer hub.Shutdown()

	// Spoke: solicits a leaf node connection up to the hub.
	spoke, _ := runSolicitLeafServer(&lo)
	defer spoke.Shutdown()

	// Wait until the leaf connection is established on both ends.
	checkLeafNodeConnected(t, hub)
	checkLeafNodeConnected(t, spoke)

	const numInboxes = 10

	// Client on the spoke with 10 inbox subscriptions.
	ncSpoke := natsConnect(t, spoke.ClientURL())
	defer ncSpoke.Close()
	spokeInboxes := make([]string, numInboxes)
	for i := range spokeInboxes {
		spokeInboxes[i] = nats.NewInbox()
		natsSubSync(t, ncSpoke, spokeInboxes[i])
	}
	natsFlush(t, ncSpoke)

	// Client on the hub with 10 inbox subscriptions.
	ncHub := natsConnect(t, hub.ClientURL())
	defer ncHub.Close()
	hubInboxes := make([]string, numInboxes)
	for i := range hubInboxes {
		hubInboxes[i] = nats.NewInbox()
		natsSubSync(t, ncHub, hubInboxes[i])
	}
	natsFlush(t, ncHub)

	// Classify every subscription living in a server's global account by origin:
	//   - local:  registered by a directly-connected client (kind CLIENT).
	//   - remote: registered on behalf of the other server's interest over the
	//             leaf connection (kind LEAF), excluding the internal "$LDS."
	//             loop-detection subscription.
	//   - lds:    the internal "$LDS." loop-detection subscription (also a LEAF
	//             sub, but not user interest).
	subBreakdown := func(s *Server) (local, remote, lds int) {
		acc := s.globalAccount()
		var subs []*subscription
		acc.sl.All(&subs)
		for _, sub := range subs {
			switch sub.client.kind {
			case CLIENT:
				local++
			case LEAF:
				if bytes.HasPrefix(sub.subject, []byte(leafNodeLoopDetectionSubjectPrefix)) {
					lds++
				} else {
					remote++
				}
			}
		}
		return local, remote, lds
	}

	// Wait for the interest graph to propagate across the leaf connection and
	// assert the local/remote split on each server.
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		if local, remote, _ := subBreakdown(spoke); local != numInboxes || remote != numInboxes {
			return fmt.Errorf("spoke: want %d local + %d remote, got %d local + %d remote",
				numInboxes, numInboxes, local, remote)
		}
		if local, remote, _ := subBreakdown(hub); local != numInboxes || remote != numInboxes {
			return fmt.Errorf("hub: want %d local + %d remote, got %d local + %d remote",
				numInboxes, numInboxes, local, remote)
		}
		return nil
	})

	// Explicitly confirm the subject interest graph propagated: every inbox
	// subscribed on one side must have matching interest on the other side.
	for _, subj := range hubInboxes {
		if !spoke.globalAccount().SubscriptionInterest(subj) {
			t.Fatalf("spoke is missing propagated interest for hub inbox %q", subj)
		}
	}
	for _, subj := range spokeInboxes {
		if !hub.globalAccount().SubscriptionInterest(subj) {
			t.Fatalf("hub is missing propagated interest for spoke inbox %q", subj)
		}
	}

	// Report the full picture for both servers.
	sLocal, sRemote, sLDS := subBreakdown(spoke)
	hLocal, hRemote, hLDS := subBreakdown(hub)
	t.Logf("spoke: total=%d (local=%d, remote=%d, lds=%d)",
		spoke.NumSubscriptions(), sLocal, sRemote, sLDS)
	t.Logf("hub:   total=%d (local=%d, remote=%d, lds=%d)",
		hub.NumSubscriptions(), hLocal, hRemote, hLDS)
}
