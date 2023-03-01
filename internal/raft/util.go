package raft

import (
	"math/rand"
	"time"
)

const (
	// Server states
	Leader    state = "Leader"
	Candidate state = "Candidate"
	Follower  state = "Follower"

	// Null votes
	nullVote = -1

	// Times (milliseconds)
	minElectionTimeout = 1000
	maxElectionTimeout = 1500
)

type state string

// Return a randomly initialized election time
func initElectionAlarm() time.Time {
	ms := minElectionTimeout + (rand.Int63() % (maxElectionTimeout - minElectionTimeout))
	timer := time.Now().Add(time.Duration(ms) * time.Millisecond)
	return timer
}
