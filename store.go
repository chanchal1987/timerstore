package timerstore

import (
	"sync"
	"time"
)

// Event is an interface that defines a method for retrieving the expiration
// time of an event. Any type that implements this interface can be used as an
// event in the Store.
type Event interface {
	ExpireAt() time.Time
}

// Store is an interface that defines methods for starting and canceling events.
// It uses a generic ID type for identifying events, and a generic Event type
// for the events themselves.
type Store[ID comparable, E Event] interface {
	Start(id ID, event E, atExpire func()) error
	Cancel(id ID) (event E, cancelled bool)
}

type data[E Event] struct {
	event E
	timer *time.Timer
}

var _ Store[any, Event] = &Simple[any, Event]{}

// Simple is an in-memory Store implementation using sync. Map for
// concurrency-safe storage.
type Simple[ID comparable, E Event] struct{ m sync.Map }

// Start stores the event and sets a timer to call atExpire when the event
// expires. It uses time. AfterFunc to schedule the expiration.
func (s *Simple[ID, E]) Start(id ID, event E, atExpire func()) error {
	s.m.Store(id, &data[E]{
		event: event,
		timer: time.AfterFunc(time.Until(event.ExpireAt()), func() {
			s.m.Delete(id)
			atExpire()
		}),
	})

	return nil
}

// Cancel stops the timer for the given id and removes the event from the store.
// It uses sync. Map to safely load and delete the event in a concurrent
// environment.
func (s *Simple[ID, E]) Cancel(id ID) (E, bool) {
	if v, ok := s.m.Load(id); ok {
		d := v.(*data[E])
		d.timer.Stop()
		s.m.Delete(id)
		return d.event, true
	}

	var zeroE E
	return zeroE, false
}

// DB is an interface that defines methods for storing and deleting events in a
// persistent storage. It is used by the Persistent store to interact with the
// underlying database or any other persistent storage mechanism.
type DB[ID any, E Event] interface {
	Put(ID, E) error
	Delete(ID, E)
}

var _ Store[any, Event] = &Persistent[any, Event]{}

// Persistent implements the Store interface using both persistent storage (DB)
// and in-memory storage.
type Persistent[ID comparable, E Event] struct {
	db DB[ID, E]
	s  Simple[ID, E]
}

// NewPersistentStore creates a new Persistent store with the given DB.
// It initializes the Persistent store with the provided DB for persistent
// storage.
func NewPersistentStore[ID comparable, E Event](db DB[ID, E]) *Persistent[ID, E] {
	return &Persistent[ID, E]{db: db}
}

// Start stores the event in the persistent storage (db) and the in-memory
// store (s). It first puts the event in the persistent storage using db.Put.
// Then, it starts the event in the in-memory store using s.Start. When the
// event expires, it deletes the event from the persistent storage and calls
// atExpire.
func (p *Persistent[ID, E]) Start(id ID, event E, atExpire func()) error {
	if err := p.db.Put(id, event); err != nil {
		return err
	}

	return p.s.Start(id, event, func() {
		p.db.Delete(id, event)
		atExpire()
	})
}

// Cancel stops the timer for the given id and removes the event from both the
// in-memory store (s) ans the persistent storage (db). It first cancels the
// event in the in-memory store using s.Cancel. If the event was successfully
// cancelled in the in-memory store, it then deletes the event from the
// persistent storage using db.Delete.
func (p *Persistent[ID, E]) Cancel(id ID) (E, bool) {
	event, ok := p.s.Cancel(id)
	if !ok {
		var zeroE E
		return zeroE, false
	}

	p.db.Delete(id, event)
	return event, true
}
