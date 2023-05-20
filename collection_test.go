package memdb

import (
	"github.com/go-faker/faker/v4"
	"github.com/siyul-park/memdb/internal/util"
	"github.com/stretchr/testify/assert"
	"testing"
)

const (
	benchmarkSetSize = 1000
)

func TestCollection_Name(t *testing.T) {
	coll := newCollection(faker.Name())

	name := coll.Name()
	assert.NotEmpty(t, name)
}

func TestCollection_Indexes(t *testing.T) {
	coll := newCollection(faker.Name())

	indexes := coll.Indexes()
	assert.NotNil(t, indexes)
}

func TestCollection_Watch(t *testing.T) {
	coll := newCollection(faker.Name())

	id := coll.Watch(func(event Event, val any) {
		if event == EventInsert {
			assert.IsType(t, val, map[string]any{})
		} else if event == EventUpdate {
			assert.IsType(t, val, map[string]any{})
		} else if event == EventDelete {
			assert.IsType(t, val, "")
		}
	})
	assert.Greater(t, id, 0)

	doc := map[string]any{
		"id":      faker.UUIDHyphenated(),
		"version": 0,
	}

	_, err := coll.InsertOne(doc)
	assert.NoError(t, err)

	_, err = coll.UpdateOne(Where("id").EQ(doc["id"]), map[string]any{"version": 1})
	assert.NoError(t, err)

	_, err = coll.DeleteOne(Where("id").EQ(doc["id"]))
	assert.NoError(t, err)
}

func TestCollection_Unwatch(t *testing.T) {
	coll := newCollection(faker.Name())

	id := coll.Watch(func(event Event, val any) {})

	coll.Unwatch(id)
}

func TestCollection_InsertOne(t *testing.T) {
	coll := newCollection(faker.Name())

	doc := map[string]any{
		"id": faker.UUIDHyphenated(),
	}

	id, err := coll.InsertOne(doc)
	assert.NoError(t, err)
	assert.Equal(t, doc["id"], id)
}

func TestCollection_InsertMany(t *testing.T) {
	coll := newCollection(faker.Name())

	doc := map[string]any{
		"id": faker.UUIDHyphenated(),
	}

	ids, err := coll.InsertMany([]map[string]any{doc})
	assert.NoError(t, err)
	assert.Len(t, ids, 1)
	assert.Equal(t, doc["id"], ids[0])
}

func TestCollection_UpdateOne(t *testing.T) {
	coll := newCollection(faker.Name())

	t.Run("options.Upsert = true", func(t *testing.T) {
		doc := map[string]any{
			"id":      faker.UUIDHyphenated(),
			"version": 0,
		}

		ok, err := coll.UpdateOne(Where("id").EQ(doc["id"]), map[string]any{"version": 1}, util.Ptr(UpdateOptions{
			Upsert: util.Ptr(true),
		}))
		assert.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("options.Upsert = false", func(t *testing.T) {
		doc := map[string]any{
			"id":      faker.UUIDHyphenated(),
			"version": 0,
		}

		ok, err := coll.UpdateOne(Where("id").EQ(doc["id"]), map[string]any{"version": 1}, util.Ptr(UpdateOptions{
			Upsert: util.Ptr(false),
		}))
		assert.NoError(t, err)
		assert.False(t, ok)

		_, err = coll.InsertOne(doc)
		assert.NoError(t, err)

		ok, err = coll.UpdateOne(Where("id").EQ(doc["id"]), map[string]any{"version": 1}, util.Ptr(UpdateOptions{
			Upsert: util.Ptr(false),
		}))
		assert.NoError(t, err)
		assert.True(t, ok)
	})
}

func TestCollection_UpdateMany(t *testing.T) {
	coll := newCollection(faker.Name())

	t.Run("options.Upsert = true", func(t *testing.T) {
		doc := map[string]any{
			"id":      faker.UUIDHyphenated(),
			"version": 0,
		}

		count, err := coll.UpdateMany(Where("id").EQ(doc["id"]), map[string]any{"version": 1}, util.Ptr(UpdateOptions{
			Upsert: util.Ptr(true),
		}))
		assert.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("options.Upsert = false", func(t *testing.T) {
		doc := map[string]any{
			"id":      faker.UUIDHyphenated(),
			"version": 0,
		}

		count, err := coll.UpdateMany(Where("id").EQ(doc["id"]), map[string]any{"version": 1}, util.Ptr(UpdateOptions{
			Upsert: util.Ptr(false),
		}))
		assert.NoError(t, err)
		assert.Equal(t, 0, count)

		_, err = coll.InsertOne(doc)
		assert.NoError(t, err)

		count, err = coll.UpdateMany(Where("id").EQ(doc["id"]), map[string]any{"version": 1}, util.Ptr(UpdateOptions{
			Upsert: util.Ptr(false),
		}))
		assert.NoError(t, err)
		assert.Equal(t, 1, count)
	})
}

func TestCollection_DeleteOne(t *testing.T) {
	coll := newCollection(faker.Name())

	doc := map[string]any{
		"id": faker.UUIDHyphenated(),
	}

	ok, err := coll.DeleteOne(Where("id").EQ(doc["id"]))
	assert.NoError(t, err)
	assert.False(t, ok)

	_, err = coll.InsertOne(doc)
	assert.NoError(t, err)

	ok, err = coll.DeleteOne(Where("id").EQ(doc["id"]))
	assert.NoError(t, err)
	assert.True(t, ok)

	ok, err = coll.DeleteOne(Where("id").EQ(doc["id"]))
	assert.NoError(t, err)
	assert.False(t, ok)
}

func TestCollection_DeleteMany(t *testing.T) {
	coll := newCollection(faker.Name())

	doc := map[string]any{
		"id": faker.UUIDHyphenated(),
	}

	count, err := coll.DeleteMany(Where("id").EQ(doc["id"]))
	assert.NoError(t, err)
	assert.Equal(t, 0, count)

	_, err = coll.InsertOne(doc)
	assert.NoError(t, err)

	count, err = coll.DeleteMany(Where("id").EQ(doc["id"]))
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	count, err = coll.DeleteMany(Where("id").EQ(doc["id"]))
	assert.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestCollection_FindOne(t *testing.T) {
	coll := newCollection(faker.Name())

	doc := map[string]any{
		"id": faker.UUIDHyphenated(),
	}

	res, err := coll.FindOne(Where("id").EQ(doc["id"]))
	assert.NoError(t, err)
	assert.Nil(t, res)

	_, err = coll.InsertOne(doc)
	assert.NoError(t, err)

	res, err = coll.FindOne(Where("id").EQ(doc["id"]))
	assert.NoError(t, err)
	assert.Equal(t, doc, res)
}

func TestCollection_FindMany(t *testing.T) {
	coll := newCollection(faker.Name())

	doc := map[string]any{
		"id": faker.UUIDHyphenated(),
	}

	res, err := coll.FindMany(Where("id").EQ(doc["id"]))
	assert.NoError(t, err)
	assert.Len(t, res, 0)

	_, err = coll.InsertOne(doc)
	assert.NoError(t, err)

	res, err = coll.FindMany(Where("id").EQ(doc["id"]))
	assert.NoError(t, err)
	assert.Len(t, res, 1)
	assert.Equal(t, doc, res[0])
}

func TestCollection_Drop(t *testing.T) {
	coll := newCollection(faker.Name())

	_ = coll.Watch(func(event Event, val any) {})

	_, err := coll.InsertOne(map[string]any{
		"id": faker.UUIDHyphenated(),
	})
	assert.NoError(t, err)

	coll.Drop()

	many, err := coll.FindMany(nil)
	assert.NoError(t, err)
	assert.Len(t, many, 0)
}

func BenchmarkCollection_InsertOne(b *testing.B) {
	coll := newCollection(faker.Name())

	for i := 0; i < b.N; i++ {
		_, err := coll.InsertOne(map[string]any{
			"id":   faker.UUIDHyphenated(),
			"type": faker.Word(),
			"name": faker.UUIDHyphenated(),
		})
		assert.NoError(b, err)
	}
}

func BenchmarkCollection_InsertMany(b *testing.B) {
	coll := newCollection(faker.Name())

	for i := 0; i < b.N; i++ {
		var docs []map[string]any
		for j := 0; j < 10; j++ {
			docs = append(docs, map[string]any{
				"id":   faker.UUIDHyphenated(),
				"type": faker.Word(),
				"name": faker.UUIDHyphenated(),
			})
		}

		_, err := coll.InsertMany(docs)
		assert.NoError(b, err)
	}
}

func BenchmarkCollection_UpdateOne(b *testing.B) {
	coll := newCollection(faker.Name())

	b.StopTimer()

	for i := 0; i < benchmarkSetSize; i++ {
		_, _ = coll.InsertOne(map[string]any{
			"id":   faker.UUIDHyphenated(),
			"type": faker.Word(),
			"name": faker.UUIDHyphenated(),
		})
	}

	v := map[string]any{
		"id":   faker.UUIDHyphenated(),
		"type": faker.Word(),
		"name": faker.UUIDHyphenated(),
	}

	_, err := coll.InsertOne(v)
	assert.NoError(b, err)

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_, err := coll.UpdateOne(Where("id").EQ(v["id"]), map[string]any{
			"type": faker.Word(),
			"name": faker.UUIDHyphenated(),
		})
		assert.NoError(b, err)
	}
}

func BenchmarkCollection_UpdateMany(b *testing.B) {
	coll := newCollection(faker.Name())

	for i := 0; i < benchmarkSetSize; i++ {
		_, _ = coll.InsertOne(map[string]any{
			"id":   faker.UUIDHyphenated(),
			"type": faker.Word(),
			"name": faker.UUIDHyphenated(),
		})
	}

	v := map[string]any{
		"id":   faker.UUIDHyphenated(),
		"type": faker.Word(),
		"name": faker.UUIDHyphenated(),
	}

	var docs []map[string]any
	for j := 0; j < 10; j++ {
		docs = append(docs, map[string]any{
			"id":   faker.UUIDHyphenated(),
			"type": faker.Word(),
			"name": v["name"],
		})
	}
	_, err := coll.InsertMany(docs)
	assert.NoError(b, err)

	_, err = coll.InsertOne(v)
	assert.NoError(b, err)

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_, err := coll.UpdateMany(Where("name").EQ(v["name"]), map[string]any{
			"type": faker.Word(),
			"name": faker.UUIDHyphenated(),
		})
		assert.NoError(b, err)
	}
}

func BenchmarkCollection_DeleteOne(b *testing.B) {
	coll := newCollection(faker.Name())

	v := map[string]any{
		"id":   faker.UUIDHyphenated(),
		"type": faker.Word(),
		"name": faker.UUIDHyphenated(),
	}

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		_, err := coll.InsertOne(v)
		assert.NoError(b, err)
		b.StartTimer()

		_, err = coll.DeleteOne(Where("id").EQ(v["id"]))
		assert.NoError(b, err)
	}
}

func BenchmarkCollection_DeleteMany(b *testing.B) {
	coll := newCollection(faker.Name())

	v := map[string]any{
		"id":   faker.UUIDHyphenated(),
		"type": faker.Word(),
		"name": faker.UUIDHyphenated(),
	}

	var docs []map[string]any
	for j := 0; j < 10; j++ {
		docs = append(docs, map[string]any{
			"id":   faker.UUIDHyphenated(),
			"type": faker.Word(),
			"name": v["name"],
		})
	}

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		_, err := coll.InsertMany(docs)
		assert.NoError(b, err)

		_, err = coll.InsertOne(v)
		assert.NoError(b, err)

		b.StartTimer()

		_, err = coll.DeleteMany(Where("name").EQ(v["name"]))
		assert.NoError(b, err)
	}
}

func BenchmarkCollection_FindOne(b *testing.B) {
	b.Run("with index", func(b *testing.B) {
		coll := newCollection(faker.Name())

		b.StopTimer()

		for i := 0; i < benchmarkSetSize; i++ {
			_, _ = coll.InsertOne(map[string]any{
				"id":   faker.UUIDHyphenated(),
				"type": faker.Word(),
				"name": faker.UUIDHyphenated(),
			})
		}

		v := map[string]any{
			"id":   faker.UUIDHyphenated(),
			"type": faker.Word(),
			"name": faker.UUIDHyphenated(),
		}

		_, err := coll.InsertOne(v)
		assert.NoError(b, err)

		b.StartTimer()

		for i := 0; i < b.N; i++ {
			_, err := coll.FindOne(Where("id").EQ(v["id"]))
			assert.NoError(b, err)
		}
	})

	b.Run("without index", func(b *testing.B) {
		coll := newCollection(faker.Name())

		b.StopTimer()

		for i := 0; i < benchmarkSetSize; i++ {
			_, _ = coll.InsertOne(map[string]any{
				"id":   faker.UUIDHyphenated(),
				"type": faker.Word(),
				"name": faker.UUIDHyphenated(),
			})
		}

		v := map[string]any{
			"id":   faker.UUIDHyphenated(),
			"type": faker.Word(),
			"name": faker.UUIDHyphenated(),
		}

		_, err := coll.InsertOne(v)
		assert.NoError(b, err)

		b.StartTimer()

		for i := 0; i < b.N; i++ {
			_, err := coll.FindOne(Where("name").EQ(v["name"]))
			assert.NoError(b, err)
		}
	})
}

func BenchmarkCollection_FindMany(b *testing.B) {
	b.Run("with index", func(b *testing.B) {
		coll := newCollection(faker.Name())

		b.StopTimer()

		for i := 0; i < benchmarkSetSize; i++ {
			_, _ = coll.InsertOne(map[string]any{
				"id":   faker.UUIDHyphenated(),
				"type": faker.Word(),
				"name": faker.UUIDHyphenated(),
			})
		}

		v := map[string]any{
			"id":   faker.UUIDHyphenated(),
			"type": faker.Word(),
			"name": faker.UUIDHyphenated(),
		}

		var docs []map[string]any
		for j := 0; j < 10; j++ {
			docs = append(docs, map[string]any{
				"id":   faker.UUIDHyphenated(),
				"type": faker.Word(),
				"name": v["name"],
			})
		}
		_, err := coll.InsertMany(docs)
		assert.NoError(b, err)

		_, err = coll.InsertOne(v)
		assert.NoError(b, err)

		b.StartTimer()

		for i := 0; i < b.N; i++ {
			_, err := coll.FindMany(Where("id").EQ(v["id"]))
			assert.NoError(b, err)
		}
	})

	b.Run("without index", func(b *testing.B) {
		coll := newCollection(faker.Name())

		b.StopTimer()

		for i := 0; i < benchmarkSetSize; i++ {
			_, _ = coll.InsertOne(map[string]any{
				"id":   faker.UUIDHyphenated(),
				"type": faker.Word(),
				"name": faker.UUIDHyphenated(),
			})
		}

		v := map[string]any{
			"id":   faker.UUIDHyphenated(),
			"type": faker.Word(),
			"name": faker.UUIDHyphenated(),
		}

		var docs []map[string]any
		for j := 0; j < 10; j++ {
			docs = append(docs, map[string]any{
				"id":   faker.UUIDHyphenated(),
				"type": faker.Word(),
				"name": v["name"],
			})
		}
		_, err := coll.InsertMany(docs)
		assert.NoError(b, err)

		_, err = coll.InsertOne(v)
		assert.NoError(b, err)

		b.StartTimer()

		for i := 0; i < b.N; i++ {
			_, err := coll.FindMany(Where("name").EQ(v["name"]))
			assert.NoError(b, err)
		}
	})
}
