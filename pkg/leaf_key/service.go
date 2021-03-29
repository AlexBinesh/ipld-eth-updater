package leaf_key

import (
	"sync"

	"github.com/sirupsen/logrus"
)

// Repairer interface for filling in leaf keys missing from the entries signifying when a node was pruned away from state
type Repairer interface {
	Repair(wg *sync.WaitGroup, quit <-chan bool)
}

// Service struct underlying the Repairer interface
type Service struct {
	writer  Writer
	start   uint64
	stop    uint64
	current uint64
	state   bool
	storage bool
}

// NewRepairServicer creates and returns a new repair service
func NewRepairService(settings *Config) Repairer {
	return &Service{
		writer:  NewLeafKeyWriter(settings.DB),
		start:   settings.Start,
		stop:    settings.Stop,
		current: settings.Start,
		state:   settings.State,
		storage: settings.Storage,
	}
}

// Repair method to fill in leaf keys missing from removed node entries
func (s *Service) Repair(wg *sync.WaitGroup, quit <-chan bool) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := s.start; i <= s.stop; i++ {
			select {
			case <-quit:
				logrus.Info("quit signal received, exiting repair loop")
				return
			default:
			}
			if s.state {
				if err := s.writer.WriteState(i); err != nil {
					logrus.Errorf("failed to write missing state leaf keys at height %d, err: %s", i, err.Error())
				}
				logrus.Debugf("wrote missing state leaf keys at height %d", i)
			}
			if s.storage {
				if err := s.writer.WriteStorage(i); err != nil {
					logrus.Errorf("failed to write missing storage leaf keys at height %d, err: %s", i, err.Error())
				}
				logrus.Debugf("wrote missing storage leaf keys at height %d", i)
			}
		}
	}()
}
