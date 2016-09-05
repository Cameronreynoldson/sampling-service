package reservoir

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
)

var (
	// Singleton state.
	instance *ReservoirState
	// Used for singleton state initialization.
	once sync.Once
)

type Reservoir struct {
	// The K current integers in the reservoir.
	data []int
	// Initial value of K.
	k int
	// Increasing value of N.
	n int
}

type ReservoirState struct {
	// Map <session id, reservoir>.
	sessionToReservoir map[uint64]*Reservoir
	// General lock for the above map.
	sessionToReservoirLock sync.RWMutex
	// Map <session id, lock for each reservoir>.
	sessionToSessionLock map[uint64]sync.RWMutex
	// General lock for the above map.
	sessionToSessionLockLock sync.RWMutex
	// Session ID for the next created session.
	nextSessionID *uint64
}

// Return a singleton instance of the reservoir state.
func Instance() *ReservoirState {
	once.Do(func() {
		instance = NewReservoirState()
	})
	return instance
}

// Constructor for the reservoir state.
func NewReservoirState() *ReservoirState {
	initialSessionID := uint64(1)
	return &ReservoirState{
		sessionToReservoir:   make(map[uint64]*Reservoir),
		sessionToSessionLock: make(map[uint64]sync.RWMutex),
		nextSessionID:        &initialSessionID,
	}
}

// Atomic method to create a reservoir given an input array of ints
// and store it in the ReservoirState. This method also returns the session
// id for the new reservoir.
func (s *ReservoirState) AtomicCreateReservoir(reservoir []int) uint64 {
	res := &Reservoir{
		data: reservoir,
		k:    len(reservoir),
		n:    len(reservoir),
	}

	state := Instance()
	sessionID := state.AtomicGetSessionIDAndIncrement()
	state.sessionToReservoirLock.Lock()
	state.sessionToReservoir[sessionID] = res
	state.sessionToSessionLockLock.Lock()
	state.sessionToSessionLock[sessionID] = sync.RWMutex{}
	state.sessionToSessionLockLock.Unlock()
	state.sessionToReservoirLock.Unlock()
	return sessionID
}

// Atomic method to update a reservoir with a given value. Here we will
// take in an integer, determine whether or not we will add the integer,
// update the reservoir if necessary and increment N.
func (s *ReservoirState) AtomicUpdateReservoir(sessionID uint64,
	integerToAdd int) error {
	state := Instance()
	reservoirLock, found := state.sessionToSessionLock[sessionID]
	if !found {
		err := fmt.Errorf("Session ID %v not found.", sessionID)
		return err
	}

	reservoirLock.Lock()
	state.sessionToReservoirLock.RLock()
	reservoir := state.sessionToReservoir[sessionID]
	state.sessionToReservoirLock.RUnlock()
	// Business logic: determines whether or not we add
	// integerToAdd to the sample.
	reservoir.n += 1
	randomIndex := rand.Intn(reservoir.n)
	if randomIndex < reservoir.k {
		reservoir.data[randomIndex] = integerToAdd
	}
	reservoirLock.Unlock()
	return nil
}

// Atomic method that ends a reservoir sampling session. This will fetch the current
// reservoir for a given id and delete the reservoir from the state.
func (s *ReservoirState) AtomicEndReservoirSession(sessionID uint64) ([]int, error) {
	state := Instance()
	reservoirLock, found := state.sessionToSessionLock[sessionID]
	if !found {
		err := fmt.Errorf("Session ID %v not found.", sessionID)
		return nil, err
	}

	// Fetch final reservoir and delete it from the state.
	reservoirLock.Lock()
	state.sessionToReservoirLock.Lock()
	reservoir := state.sessionToReservoir[sessionID]
	data := reservoir.data
	delete(state.sessionToReservoir, sessionID)
	state.sessionToReservoirLock.Unlock()
	reservoirLock.Unlock()

	// Delete lock mapping for reservoir.
	state.sessionToSessionLockLock.Lock()
	delete(state.sessionToSessionLock, sessionID)
	state.sessionToSessionLockLock.Unlock()

	return data, nil
}

// Atomic method to return the current nextSessionID and increment it
// so that the next creation of a reservoir may use nextSessionID.
func (s *ReservoirState) AtomicGetSessionIDAndIncrement() uint64 {
	state := Instance()
	sessionID := atomic.LoadUint64(state.nextSessionID)
	atomic.AddUint64(state.nextSessionID, 1)
	return sessionID
}
