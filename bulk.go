// Package mongobulk wraps mgo driver's Bulk type to enables batches of unlimited size.
package mongobulk

import (
	"github.com/fluxio/multierror"
	"gopkg.in/mgo.v2"
)

// MaxOpsPerBatch is the maximum number of operations MongoDB supports in a bulk request.
const MaxOpsPerBatch = 1000

type Config struct {
	// OpsPerBatch is a number of operations to be queued up in a bulk request.
	// The batch will be executed when this many operations get queued.
	// Cannot exceed MaxOpsPerBatch, otherwise MongoDB will refuse to execute anything.
	// Defaults to MaxOpsPerBatch.
	OpsPerBatch int
}

type Bulk struct {
	Config
	Collection *mgo.Collection

	bulk     *mgo.Bulk
	count    int
	errs     multierror.Accumulator
	finished bool
}

func New(collection *mgo.Collection, config Config) *Bulk {
	if config.OpsPerBatch == 0 {
		config.OpsPerBatch = MaxOpsPerBatch
	}

	return &Bulk{
		Config:     config,
		Collection: collection,

		bulk:  nil,
		count: 0,
	}
}

func (b *Bulk) Insert(docs ...interface{}) {
	for _, doc := range docs {
		b.prepare()
		b.bulk.Insert(doc)
	}
}

func (b *Bulk) Remove(selectors ...interface{}) {
	for _, sel := range selectors {
		b.prepare()
		b.bulk.Remove(sel)
	}
}

func (b *Bulk) RemoveAll(selectors ...interface{}) {
	for _, sel := range selectors {
		b.prepare()
		b.bulk.RemoveAll(sel)
	}
}

func (b *Bulk) Update(pairs ...interface{}) {
	var last interface{}
	for idx, doc := range pairs {
		if idx%2 == 0 {
			last = doc
		} else {
			b.prepare()
			b.bulk.Update(last, doc)
		}
	}
}

func (b *Bulk) UpdateAll(pairs ...interface{}) {
	var last interface{}
	for idx, doc := range pairs {
		if idx%2 == 0 {
			last = doc
		} else {
			b.prepare()
			b.bulk.UpdateAll(last, doc)
		}
	}
}

func (b *Bulk) Upsert(pairs ...interface{}) {
	var last interface{}
	for idx, doc := range pairs {
		if idx%2 == 0 {
			last = doc
		} else {
			b.prepare()
			b.bulk.Upsert(last, doc)
		}
	}
}

func (b *Bulk) Finish() error {
	if b.finished {
		panic("attempting to finish a bulk twice")
	}
	b.finished = true
	b.flush()
	return b.errs.Error()
}

func (b *Bulk) flush() {
	if b.bulk != nil {
		_, err := b.bulk.Run()
		if err != nil {
			b.errs.Push(err)
		}
		b.bulk = nil
		b.count = 0
	}
}

func (b *Bulk) prepare() {
	if b.finished {
		panic("performing an operation on a finished bulk")
	}
	if b.count >= b.OpsPerBatch {
		b.flush()
	}
	b.count++
	if b.bulk == nil {
		b.bulk = b.Collection.Bulk()
		b.bulk.Unordered()
	}
}
