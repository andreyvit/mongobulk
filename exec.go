package mongobulk

import (
	"gopkg.in/mgo.v2"
)

func Exec(db *mgo.Database, collectionName string, config Config, f func(bulk *Bulk)) error {
	coll := db.C(collectionName)
	bulk := New(coll, config)
	f(bulk)
	return bulk.Finish()
}
