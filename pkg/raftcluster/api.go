package raftcluster

import (
	"sync"
)

// EventNotifier is an interface that groups ClusterDiscover Interface and OnEvent Method.
// EventNotifier is used by the default ClusterDiscoverService implementation to send notification about cluster change.
type EventNotifier interface {
	ClusterDiscover
	OnEvent(eventName string, payload []byte)
}

// UserEvent is an interface that wraps the basic SendEvent and OnEvent Method.
// SendEvent is used for generating a custom user type event inside the cluster.
type UserEvent interface {
	SendEvent(name string, payload []byte) error
}

// ClusterDiscover is an interface that wraps the basic OnChangeEvent Method.
// OnChangeEvent notifies the provider when there is a change in the cluster dynamics.
type ClusterDiscover interface {
	OnChangeEvent(event MemberEvent, information MemberInformation)
}

// MemberInformation stores information object for one (member) node in cluster.
type MemberInformation struct {
	NodeName string
	Tags     map[string]string
}

// Clone Deep clone's the MemberInfo Object.
func (mi MemberInformation) Clone() MemberInformation {
	m := make(map[string]string)
	for k, v := range mi.Tags {
		m[k] = v
	}

	return MemberInformation{
		NodeName: mi.NodeName,
		Tags:     mi.Tags,
	}
}

// inMemoryStore provides in memory storage for clustering .
type inMemoryStore struct {
	isLeader int32
	// Notifies about the Event to UserSpace.
	en EventNotifier

	l       sync.RWMutex
	members map[string]MemberInformation
}

func newInMemoryStore(en EventNotifier) *inMemoryStore {
	return &inMemoryStore{
		l:       sync.RWMutex{},
		members: make(map[string]MemberInformation),
		en:      en,
	}
}

// OnEvent callback's the provider with the named event and it's payload.
func (m *inMemoryStore) OnEvent(eventName string, payload []byte) {
	m.en.OnEvent(eventName, payload)
}

func (m *inMemoryStore) OnChangeEvent(event MemberEvent, info MemberInformation) {
	switch event {
	case MemberEventJoin:
		m.join(info)
	case MemberEventLeave:
		m.leave(info)
	}

	m.en.OnChangeEvent(event, info)
}

func (m *inMemoryStore) join(mi MemberInformation) {
	m.l.Lock()
	defer m.l.Unlock()
	// clone the tags map for concurrent safe ops in other part.
	m.members[mi.NodeName] = mi.Clone()
}

func (m *inMemoryStore) leave(mi MemberInformation) {
	m.l.Lock()
	defer m.l.Unlock()
	delete(m.members, mi.NodeName)
}
