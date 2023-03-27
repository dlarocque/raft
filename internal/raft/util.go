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
	minElectionTimeout = 800
	maxElectionTimeout = 1600
)

type state string

func min(a, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}

// Return a randomly initialized election time
func initElectionAlarm() time.Time {
	ms := rand.Intn(maxElectionTimeout-minElectionTimeout) + minElectionTimeout
	timer := time.Now().Add(time.Duration(ms) * time.Millisecond)
	return timer
}
