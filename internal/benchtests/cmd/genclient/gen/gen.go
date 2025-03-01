package gen

import (
	"math/rand"
	"sync/atomic"
	"time"
)

type Setter interface {
	Set()
}

type Getter interface {
	Get()
}

type RatioRobin struct {
	setter   Setter
	getter   Getter
	setOps   uint64
	getOps   uint64
	totalOps uint64
	count    atomic.Uint64

	// it's a precomputed seq randomized for set and get ops in the same ratio.
	opSequence []bool
}

func NewRatioRobin(setter Setter, getter Getter, setOps, getOps uint64) *RatioRobin {
	totalOps := setOps + getOps
	opSequence := make([]bool, totalOps)

	for i := uint64(0); i < setOps; i++ {
		opSequence[i] = true
	}

	for i := setOps; i < totalOps; i++ {
		opSequence[i] = false
	}

	random := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	random.Shuffle(len(opSequence), func(i, j int) {
		opSequence[i], opSequence[j] = opSequence[j], opSequence[i]
	})

	return &RatioRobin{
		setter:     setter,
		getter:     getter,
		setOps:     setOps,
		getOps:     getOps,
		opSequence: opSequence,
		totalOps:   totalOps,
	}
}

func (r *RatioRobin) Gen() {
	r.count.Add(1)
	i := r.count.Load() % r.totalOps

	switch r.opSequence[i] {
	case true:
		r.setter.Set()
	default:
		r.getter.Get()
	}
}
