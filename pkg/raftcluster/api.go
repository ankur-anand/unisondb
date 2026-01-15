package raftcluster

import "time"

// ClusterDiscover is an interface that wraps the basic OnChangeEvent Method.
// OnChangeEvent notifies the provider when there is a change in the cluster dynamics.
type ClusterDiscover interface {
	OnChangeEvent(event MemberEvent, information MemberInformation)
	ReconcileMembers([]MemberInformation)
}

type MemberStatus uint8

const (
	MemberStatusUnknown MemberStatus = iota
	MemberStatusAlive
	MemberStatusFailed
	MemberStatusLeft
	MemberStatusReaped
)

// MemberInformation stores information object for one (member) node in cluster.
type MemberInformation struct {
	NodeName   string
	Tags       map[string]string
	Status     MemberStatus
	UpdatedAt  time.Time
	LastSeenAt time.Time
}

// Clone returns a deep copy of the MemberInformation.
func (mi MemberInformation) Clone() MemberInformation {
	tags := make(map[string]string, len(mi.Tags))
	for k, v := range mi.Tags {
		tags[k] = v
	}

	return MemberInformation{
		NodeName:   mi.NodeName,
		Tags:       tags,
		Status:     mi.Status,
		UpdatedAt:  mi.UpdatedAt,
		LastSeenAt: mi.LastSeenAt,
	}
}
