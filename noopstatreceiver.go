// Copyright (c) 2016 Twitch Interactive

package kinsumer

import "time"

// NoopStatReceiver is a statreceiver that doesn't do anything, use it if you do not want to collect
// stats, or as a base if you want to just collect a subset of stats
type NoopStatReceiver struct {
}

// Checkpoint implementation that doesn't do anything
func (*NoopStatReceiver) Checkpoint() {}

// EventToClient implementation that doesn't do anything
func (*NoopStatReceiver) EventToClient(inserted, retrieved time.Time, size int) {}

// EventsFromKinesis implementation that doesn't do anything
func (*NoopStatReceiver) EventsFromKinesis(num int, shardID string, lag time.Duration) {}
