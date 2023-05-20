package memdb

import (
	"github.com/pkg/errors"
	"github.com/siyul-park/memdb/internal/pool"
	"github.com/siyul-park/memdb/internal/util/reflectutil"
	"sync"
)

type (
	IndexView struct {
		names  []string
		models []IndexModel
		data   []*sync.Map
		lock   sync.RWMutex
	}

	IndexModel struct {
		Keys    []string
		Name    string
		Unique  bool
		Partial *Filter
	}
)

const (
	keyID = "id"
)

var (
	ErrCodeIndexConflict = "index_conflict"
	ErrCodeIndexNotFound = "index_notfound"

	ErrIndexConflict = errors.New(ErrCodeIndexConflict)
	ErrIndexNotFound = errors.New(ErrCodeIndexNotFound)
)

func newIndexView() *IndexView {
	iv := &IndexView{
		names:  nil,
		models: nil,
		data:   nil,
		lock:   sync.RWMutex{},
	}
	iv.Create(IndexModel{
		Keys:    []string{"id"},
		Name:    "_id",
		Unique:  true,
		Partial: nil,
	})

	return iv
}

func (iv *IndexView) List() []IndexModel {
	iv.lock.RLock()
	defer iv.lock.RUnlock()

	return iv.models
}

func (iv *IndexView) Create(index IndexModel) {
	iv.lock.Lock()
	defer iv.lock.Unlock()

	name := index.Name

	for i, n := range iv.names {
		if n == name {
			iv.names = append(iv.names[:i], iv.names[i+1:]...)
			iv.models = append(iv.models[:i], iv.models[i+1:]...)
			iv.data = append(iv.data[:i], iv.data[i+1:]...)
		}
	}

	iv.names = append(iv.names, name)
	iv.models = append(iv.models, index)
	iv.data = append(iv.data, pool.GetMap())
}

func (iv *IndexView) Drop(name string) {
	iv.lock.Lock()
	defer iv.lock.Unlock()

	for i, n := range iv.names {
		if n == name {
			iv.names = append(iv.names[:i], iv.names[i+1:]...)
			iv.models = append(iv.models[:i], iv.models[i+1:]...)
			iv.data = append(iv.data[:i], iv.data[i+1:]...)
		}
	}
}

func (iv *IndexView) insertMany(documents []map[string]any) error {
	iv.lock.Lock()
	defer iv.lock.Unlock()

	for i, doc := range documents {
		if err := iv.insertOne(doc); err != nil {
			for i--; i >= 0; i-- {
				_ = iv.deleteOne(doc)
			}
			return err
		}
	}
	return nil
}

func (iv *IndexView) deleteMany(documents []map[string]any) error {
	iv.lock.Lock()
	defer iv.lock.Unlock()

	for i, doc := range documents {
		if err := iv.deleteOne(doc); err != nil {
			for ; i >= 0; i-- {
				_ = iv.insertOne(doc)
			}
			return err
		}
	}
	return nil
}

func (iv *IndexView) deleteAll() {
	iv.lock.Lock()
	defer iv.lock.Unlock()

	iv.data = nil
}

func (iv *IndexView) findMany(filter *Filter) ([]any, error) {
	iv.lock.RLock()
	defer iv.lock.RUnlock()

	examples, ok := filterToExample(filter)
	if !ok {
		return nil, ErrIndexNotFound
	}

	ids := pool.GetMap()
	defer pool.PutMap(ids)

	for _, example := range examples {
		if err := func() error {
			for i, model := range iv.models {
				curr := iv.data[i]

				visits := map[string]bool{}
				for k := range example {
					visits[k] = false
				}
				next := false

				var i int
				var k string
				for i, k = range model.Keys {
					if v, ok := example[k]; ok {
						visits[k] = true
						if sub, ok := curr.Load(v); ok {
							if i < len(model.Keys)-1 {
								curr = sub.(*sync.Map)
							} else {
								if model.Unique {
									ids.Store(sub, nil)
									return nil
								} else {
									sub.(*sync.Map).Range(func(key, _ any) bool {
										ids.Store(key, nil)
										return true
									})
									return nil
								}
							}
						} else {
							next = true
							break
						}
					} else {
						break
					}
				}

				for _, v := range visits {
					if !v {
						next = true
					}
				}
				if next {
					continue
				}

				var parent []*sync.Map
				parent = append(parent, curr)

				depth := len(model.Keys) - 1
				if !model.Unique {
					depth += 1
				}

				for ; i < depth; i++ {
					var children []*sync.Map
					for _, curr := range parent {
						curr.Range(func(_, value any) bool {
							children = append(children, value.(*sync.Map))
							return true
						})
					}
					parent = children
				}

				for _, curr := range parent {
					curr.Range(func(k, v any) bool {
						if !model.Unique {
							ids.Store(k, nil)
						} else {
							ids.Store(v, nil)
						}
						return true
					})
				}

				return nil
			}

			return ErrIndexNotFound
		}(); err != nil {
			return nil, err
		}
	}

	var uniqueIds []any
	ids.Range(func(key, _ any) bool {
		uniqueIds = append(uniqueIds, key)
		return true
	})
	return uniqueIds, nil
}

func (iv *IndexView) insertOne(document map[string]any) error {
	id, ok := document[keyID]
	if !ok {
		return ErrIndexConflict
	}

	for i, model := range iv.models {
		if err := func() error {
			curr := iv.data[i]

			if !parseFilter(model.Partial)(document) {
				return nil
			}

			for i, k := range model.Keys {
				v, ok := reflectutil.Get[any](document, k)
				if !ok {
					v = nil
				}
				if i < len(model.Keys)-1 {
					cm := pool.GetMap()
					sub, load := curr.LoadOrStore(v, cm)
					if load {
						pool.PutMap(cm)
					}
					curr = sub.(*sync.Map)
				} else if model.Unique {
					if r, loaded := curr.LoadOrStore(v, id); loaded && r != id {
						return ErrIndexConflict
					}
				} else {
					cm := pool.GetMap()
					r, load := curr.LoadOrStore(v, cm)
					if load {
						pool.PutMap(cm)
					}
					r.(*sync.Map).Store(id, nil)
				}
			}

			return nil
		}(); err != nil {
			_ = iv.deleteOne(document)
			return err
		}
	}

	return nil
}

func (iv *IndexView) deleteOne(document map[string]any) error {
	id, ok := document[keyID]
	if !ok {
		return ErrIndexConflict
	}

	for i, model := range iv.models {
		if err := func() error {
			curr := iv.data[i]

			if !parseFilter(model.Partial)(document) {
				return nil
			}

			var nodes []*sync.Map
			nodes = append(nodes, curr)
			var keys []any
			keys = append(keys, nil)

			for i, k := range model.Keys {
				v, ok := reflectutil.Get[any](document, k)
				if !ok {
					v = nil
				}
				if i < len(model.Keys)-1 {
					if sub, ok := curr.Load(v); ok {
						curr = sub.(*sync.Map)

						nodes = append(nodes, curr)
						keys = append(keys, v)
					} else {
						return nil
					}
				} else if model.Unique {
					if r, loaded := curr.Load(v); loaded && reflectutil.Equal(r, id) {
						curr.Delete(k)
					}
				} else {
					if r, loaded := curr.Load(v); loaded {
						nodes = append(nodes, r.(*sync.Map))
						keys = append(keys, v)
						r.(*sync.Map).Delete(id)
					}
				}
			}

			for i := len(nodes) - 1; i >= 0; i-- {
				node := nodes[i]

				empty := true
				node.Range(func(_, _ any) bool {
					empty = false
					return false
				})

				if empty && i > 0 {
					parent := nodes[i-1]
					key := keys[i]

					parent.Delete(key)
					pool.PutMap(node)
				}
			}

			return nil
		}(); err != nil {
			return err
		}
	}

	return nil
}
