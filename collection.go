package memdb

import (
	"github.com/pkg/errors"
	"github.com/siyul-park/memdb/internal/pool"
	"github.com/siyul-park/memdb/internal/util"
	"sort"
	"sync"
)

type (
	Collection struct {
		name          string
		data          *sync.Map
		indexView     *IndexView
		listeners     map[int]func(Event, any)
		dataLock      sync.RWMutex
		listenersLock sync.RWMutex
	}

	UpdateOptions struct {
		Upsert *bool
	}

	FindOptions struct {
		Limit *int
		Skip  *int
		Sorts []Sort
	}

	Event int
)

const (
	EventInsert Event = iota
	EventUpdate
	EventDelete
)

var (
	ErrCodePKNotFound   = "pk_notfound"
	ErrCodePKDuplicated = "pk_duplicated"

	ErrPKNotFound   = errors.New(ErrCodePKNotFound)
	ErrPKDuplicated = errors.New(ErrCodePKDuplicated)
)

func newCollection(name string) *Collection {
	return &Collection{
		name:          name,
		data:          pool.GetMap(),
		indexView:     newIndexView(),
		listeners:     map[int]func(Event, any){},
		dataLock:      sync.RWMutex{},
		listenersLock: sync.RWMutex{},
	}
}

func (coll *Collection) Name() string {
	coll.dataLock.RLock()
	defer coll.dataLock.RUnlock()

	return coll.name
}

func (coll *Collection) Indexes() *IndexView {
	coll.dataLock.RLock()
	defer coll.dataLock.RUnlock()

	return coll.indexView
}

func (coll *Collection) Watch(listener func(event Event, val any)) int {
	coll.listenersLock.Lock()
	defer coll.listenersLock.Unlock()

	id := 1
	for {
		if _, ok := coll.listeners[id]; ok {
			id += 1
		} else {
			break
		}
	}
	coll.listeners[id] = listener

	return id
}

func (coll *Collection) Unwatch(listenerID int) {
	coll.listenersLock.Lock()
	defer coll.listenersLock.Unlock()

	delete(coll.listeners, listenerID)
}

func (coll *Collection) InsertOne(document map[string]any) (any, error) {
	if id, err := coll.insertOne(document); err != nil {
		return nil, err
	} else {
		coll.emit(EventInsert, document)
		return id, nil
	}
}

func (coll *Collection) InsertMany(documents []map[string]any) ([]any, error) {
	if ids, err := coll.insertMany(documents); err != nil {
		return nil, err
	} else {
		for _, doc := range documents {
			coll.emit(EventInsert, doc)
		}
		return ids, nil
	}
}

func (coll *Collection) UpdateOne(filter *Filter, update map[string]any, opts ...*UpdateOptions) (bool, error) {
	opt := mergeUpdateOptions(opts)
	upsert := false
	if !util.IsNil(opt) && !util.IsNil(opt.Upsert) {
		upsert = util.UnPtr(opt.Upsert)
	}

	doc, err := coll.findOne(filter)
	if err != nil {
		return false, err
	}
	if util.IsNil(doc) && !upsert {
		return false, nil
	}

	var id any
	if !util.IsNil(doc) {
		id = doc[keyID]
	}
	if util.IsNil(id) {
		id = update[keyID]
	}
	if util.IsNil(id) {
		if examples, ok := filterToExample(filter); ok {
			for _, example := range examples {
				if v, ok := example[keyID]; ok {
					if util.IsNil(id) {
						id = v
					} else {
						return false, ErrPKDuplicated
					}
				}
			}
		}
	}
	if util.IsNil(id) {
		return false, ErrPKNotFound
	}

	if !util.IsNil(doc) {
		if _, err := coll.deleteOne(doc); err != nil {
			return false, err
		}
	}

	old := doc
	doc = map[string]any{keyID: id}
	for k, v := range update {
		doc[k] = v
	}
	if _, err := coll.insertOne(doc); err != nil {
		_, _ = coll.InsertOne(old)
		return false, err
	}

	coll.emit(EventUpdate, doc)

	return true, nil
}

func (coll *Collection) UpdateMany(filter *Filter, update map[string]any, opts ...*UpdateOptions) (int, error) {
	opt := mergeUpdateOptions(opts)
	upsert := false
	if !util.IsNil(opt) && !util.IsNil(opt.Upsert) {
		upsert = util.UnPtr(opt.Upsert)
	}

	docs, err := coll.findMany(filter)
	if err != nil {
		return 0, err
	}
	if len(docs) == 0 {
		if !upsert {
			return 0, nil
		}

		id := update[keyID]
		if util.IsNil(id) {
			if examples, ok := filterToExample(filter); ok {
				for _, example := range examples {
					if v, ok := example[keyID]; ok {
						if util.IsNil(id) {
							id = v
						} else {
							return 0, ErrPKDuplicated
						}
					}
				}
			}
		}

		doc := map[string]any{keyID: id}
		for k, v := range update {
			doc[k] = v
		}
		if _, err := coll.insertOne(doc); err != nil {
			return 0, err
		}
		return 1, nil
	}

	if _, err := coll.deleteMany(docs); err != nil {
		return 0, err
	}
	old := docs
	for i, doc := range docs {
		doc := map[string]any{keyID: doc[keyID]}
		for k, v := range update {
			doc[k] = v
		}
		docs[i] = doc
	}
	if _, err := coll.insertMany(docs); err != nil {
		_, _ = coll.insertMany(old)
		return 0, err
	}

	for _, doc := range docs {
		coll.emit(EventInsert, doc)
	}

	return len(docs), nil
}

func (coll *Collection) DeleteOne(filter *Filter) (bool, error) {
	if doc, err := coll.findOne(filter); err != nil {
		return false, err
	} else if doc, err := coll.deleteOne(doc); err != nil {
		return false, err
	} else {
		if !util.IsNil(doc) {
			if id, ok := doc[keyID]; ok {
				coll.emit(EventDelete, id)
			}
		}
		return !util.IsNil(doc), nil
	}
}

func (coll *Collection) DeleteMany(filter *Filter) (int, error) {
	if docs, err := coll.findMany(filter); err != nil {
		return 0, err
	} else if docs, err := coll.deleteMany(docs); err != nil {
		return 0, err
	} else {
		for _, doc := range docs {
			if id, ok := doc[keyID]; ok {
				coll.emit(EventDelete, id)
			}
		}
		return len(docs), nil
	}
}

func (coll *Collection) FindOne(filter *Filter, opts ...*FindOptions) (map[string]any, error) {
	return coll.findOne(filter, opts...)
}

func (coll *Collection) FindMany(filter *Filter, opts ...*FindOptions) ([]map[string]any, error) {
	return coll.findMany(filter, opts...)
}

func (coll *Collection) Drop() {
	data := func() *sync.Map {
		coll.dataLock.Lock()
		defer coll.dataLock.Unlock()

		data := coll.data
		coll.data = pool.GetMap()

		coll.indexView.deleteAll()

		return data
	}()

	data.Range(func(key, _ any) bool {
		coll.emit(EventDelete, key)
		return true
	})
}

func (coll *Collection) insertOne(document map[string]any) (any, error) {
	if ids, err := coll.insertMany([]map[string]any{document}); err != nil {
		return nil, err
	} else {
		return ids[0], nil
	}
}

func (coll *Collection) insertMany(documents []map[string]any) ([]any, error) {
	coll.dataLock.Lock()
	defer coll.dataLock.Unlock()

	var ids []any
	for _, doc := range documents {
		id, ok := doc[keyID]
		if !ok {
			return nil, ErrPKNotFound
		} else if _, ok := coll.data.Load(id); ok {
			return nil, ErrPKDuplicated
		}
		ids = append(ids, id)
	}

	if err := coll.indexView.insertMany(documents); err != nil {
		return nil, err
	}
	for i, doc := range documents {
		coll.data.Store(ids[i], doc)
	}

	return ids, nil
}

func (coll *Collection) findOne(filter *Filter, opts ...*FindOptions) (map[string]any, error) {
	opt := mergeFindOptions(append(opts, util.Ptr(FindOptions{Limit: util.Ptr(1)})))

	if docs, err := coll.findMany(filter, opt); err != nil {
		return nil, err
	} else if len(docs) > 0 {
		return docs[0], nil
	} else {
		return nil, nil
	}
}

func (coll *Collection) findMany(filter *Filter, opts ...*FindOptions) ([]map[string]any, error) {
	coll.dataLock.RLock()
	defer coll.dataLock.RUnlock()

	opt := mergeFindOptions(opts)

	limit := -1
	if !util.IsNil(opt) && !util.IsNil(opt.Limit) {
		limit = util.UnPtr(opt.Limit)
	}
	skip := 0
	if !util.IsNil(opt) && !util.IsNil(opt.Skip) {
		skip = util.UnPtr(opt.Skip)
	}
	var sorts []Sort
	if !util.IsNil(opt) && !util.IsNil(opt.Sorts) {
		sorts = opt.Sorts
	}

	match := parseFilter(filter)

	scanSize := limit
	if skip > 0 || len(sorts) > 0 {
		scanSize = -1
	}

	var docs []map[string]any

	if ids, err := coll.indexView.findMany(filter); err == nil {
		for _, id := range ids {
			if scanSize == len(docs) {
				break
			}
			if doc, ok := coll.data.Load(id); ok && match(doc.(map[string]any)) {
				docs = append(docs, doc.(map[string]any))
			}
		}
	} else {
		coll.data.Range(func(_, value any) bool {
			if scanSize == len(docs) {
				return false
			}

			if match(value.(map[string]any)) {
				docs = append(docs, value.(map[string]any))
			}
			return true
		})
	}

	if skip >= len(docs) {
		return nil, nil
	}
	if len(sorts) > 0 {
		compare := parseSorts(sorts)
		sort.Slice(docs, func(i, j int) bool {
			return compare(docs[i], docs[j])
		})
	}
	if limit >= 0 {
		if len(docs) > limit+skip {
			docs = docs[skip : limit+skip]
		} else {
			docs = docs[skip:]
		}
	}
	return docs, nil
}

func (coll *Collection) deleteOne(document map[string]any) (map[string]any, error) {
	if docs, err := coll.deleteMany([]map[string]any{document}); err != nil {
		return nil, err
	} else if len(docs) > 0 {
		return docs[0], nil
	} else {
		return nil, nil
	}
}

func (coll *Collection) deleteMany(documents []map[string]any) ([]map[string]any, error) {
	coll.dataLock.Lock()
	defer coll.dataLock.Unlock()

	var ids []any
	var docs []map[string]any
	for _, doc := range documents {
		if id, ok := doc[keyID]; !ok {
			continue
		} else {
			ids = append(ids, id)
			docs = append(docs, doc)
		}
	}

	if err := coll.indexView.deleteMany(docs); err != nil {
		return nil, err
	}

	for _, id := range ids {
		coll.data.Delete(id)
	}

	return docs, nil
}

func (coll *Collection) emit(event Event, val any) {
	coll.listenersLock.RLock()
	defer coll.listenersLock.RUnlock()

	for _, lt := range coll.listeners {
		lt(event, val)
	}
}

func mergeUpdateOptions(options []*UpdateOptions) *UpdateOptions {
	if len(options) == 0 {
		return nil
	}
	opt := &UpdateOptions{}
	for _, curr := range options {
		if util.IsNil(curr) {
			continue
		}
		if !util.IsNil(curr.Upsert) {
			opt.Upsert = curr.Upsert
		}
	}
	return opt
}

func mergeFindOptions(options []*FindOptions) *FindOptions {
	if len(options) == 0 {
		return nil
	}
	opt := &FindOptions{}
	for _, curr := range options {
		if util.IsNil(curr) {
			continue
		}
		if !util.IsNil(curr.Limit) {
			opt.Limit = curr.Limit
		}
		if !util.IsNil(curr.Skip) {
			opt.Skip = curr.Skip
		}
		if !util.IsNil(curr.Sorts) {
			opt.Sorts = curr.Sorts
		}
	}
	return opt
}
