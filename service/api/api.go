package api

import (
	"service/reservoir"
)

// API function to begin a reservoir sampling session. Takes in
// an initial reservoir, adds it to the state and returns the
// session ID.
func BeginSession(initialReservoir []int) (uint64, error) {
	state := reservoir.Instance()
	return state.AtomicCreateReservoir(initialReservoir), nil
}

// API function to add an integer to an existing reservoir
// sampling session. The business logic for computing probabilities
// and whether or not elements stay will be called inside of here,
// specifically in AtomicUpdateReservoir().
func AddInteger(sessionID uint64, integerToAdd int) error {
	state := reservoir.Instance()
	return state.AtomicUpdateReservoir(sessionID, integerToAdd)
}

// API function to end a reservoir sampling session. This takes
// in a session ID, returns the corresponding random sample and
// deletes the reservoir from the state.
func EndSession(sessionID uint64) ([]int, error) {
	state := reservoir.Instance()
	return state.AtomicEndReservoirSession(sessionID)
}
