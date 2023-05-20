package memdb

import (
	"github.com/go-faker/faker/v4"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIndexView_List(t *testing.T) {
	iv := newIndexView()

	model := IndexModel{
		Keys:    []string{"sub_key"},
		Name:    faker.UUIDHyphenated(),
		Unique:  false,
		Partial: Where("type").EQ("any"),
	}

	iv.Create(model)

	models := iv.List()
	assert.Len(t, models, 2)

	assert.Equal(t, model, models[len(models)-1])
}

func TestIndexView_Create(t *testing.T) {
	iv := newIndexView()

	model := IndexModel{
		Keys: []string{"sub_key"},
		Name: faker.UUIDHyphenated(),
	}

	iv.Create(model)
}

func TestIndexView_Drop(t *testing.T) {
	iv := newIndexView()

	model := IndexModel{
		Keys: []string{"sub_key"},
		Name: faker.UUIDHyphenated(),
	}

	iv.Create(model)

	iv.Drop(model.Name)

	models := iv.List()
	assert.Len(t, models, 1)
}

func TestIndexView_InsertMany(t *testing.T) {
	t.Run("error: nil", func(t *testing.T) {
		iv := newIndexView()

		iv.Create(IndexModel{
			Keys:    []string{"a.b", "c"},
			Name:    "a.b_c",
			Unique:  true,
			Partial: nil,
		})
		iv.Create(IndexModel{
			Keys:    []string{"d"},
			Name:    "d",
			Unique:  false,
			Partial: nil,
		})

		docs := []map[string]any{
			{
				"id": faker.UUIDHyphenated(),
				"a": map[string]any{
					"b": faker.UUIDHyphenated(),
				},
				"c": faker.UUIDHyphenated(),
				"d": faker.UUIDHyphenated(),
			},
			{
				"id": faker.UUIDHyphenated(),
				"a": map[string]any{
					"b": faker.UUIDHyphenated(),
				},
				"c": faker.UUIDHyphenated(),
				"d": faker.UUIDHyphenated(),
			},
			{
				"id": faker.UUIDHyphenated(),
				"a": map[string]any{
					"b": faker.UUIDHyphenated(),
				},
				"c": faker.UUIDHyphenated(),
			},
		}

		err := iv.insertMany(docs)
		assert.NoError(t, err)
	})

	t.Run("error: ErrIndexConflict", func(t *testing.T) {
		iv := newIndexView()

		iv.Create(IndexModel{
			Keys:    []string{"a.b", "c"},
			Name:    "a.b_c",
			Unique:  true,
			Partial: nil,
		})
		iv.Create(IndexModel{
			Keys:    []string{"d"},
			Name:    "d",
			Unique:  false,
			Partial: nil,
		})

		docs := []map[string]any{
			{
				"id": faker.UUIDHyphenated(),
				"a": map[string]any{
					"b": "0",
				},
				"c": "0",
				"d": faker.UUIDHyphenated(),
			},
			{
				"id": faker.UUIDHyphenated(),
				"a": map[string]any{
					"b": "0",
				},
				"c": "0",
				"d": faker.UUIDHyphenated(),
			},
			{
				"id": faker.UUIDHyphenated(),
				"a": map[string]any{
					"b": "1",
				},
				"c": "1",
			},
		}

		err := iv.insertMany(docs)
		assert.ErrorIs(t, err, ErrIndexConflict)
	})
}

func TestIndexView_DeleteMany(t *testing.T) {
	t.Run("error: nil", func(t *testing.T) {
		iv := newIndexView()

		iv.Create(IndexModel{
			Keys:    []string{"a.b", "c"},
			Name:    "a.b_c",
			Unique:  true,
			Partial: nil,
		})
		iv.Create(IndexModel{
			Keys:    []string{"d"},
			Name:    "d",
			Unique:  false,
			Partial: nil,
		})

		docs := []map[string]any{
			{
				"id": faker.UUIDHyphenated(),
				"a": map[string]any{
					"b": faker.UUIDHyphenated(),
				},
				"c": faker.UUIDHyphenated(),
				"d": faker.UUIDHyphenated(),
			},
			{
				"id": faker.UUIDHyphenated(),
				"a": map[string]any{
					"b": faker.UUIDHyphenated(),
				},
				"c": faker.UUIDHyphenated(),
				"d": faker.UUIDHyphenated(),
			},
			{
				"id": faker.UUIDHyphenated(),
				"a": map[string]any{
					"b": faker.UUIDHyphenated(),
				},
				"c": faker.UUIDHyphenated(),
			},
		}

		_ = iv.insertMany(docs)

		err := iv.deleteMany(docs)
		assert.NoError(t, err)
	})
}

func TestIndexView_FindMany(t *testing.T) {
	t.Run("error: nil, unique: true", func(t *testing.T) {
		iv := newIndexView()

		iv.Create(IndexModel{
			Keys:    []string{"c", "a.b"},
			Name:    "c_a.b",
			Unique:  true,
			Partial: nil,
		})
		iv.Create(IndexModel{
			Keys:    []string{"d"},
			Name:    "d",
			Unique:  false,
			Partial: nil,
		})

		docs := []map[string]any{
			{
				"id": faker.UUIDHyphenated(),
				"a": map[string]any{
					"b": faker.UUIDHyphenated(),
				},
				"c": faker.UUIDHyphenated(),
				"d": faker.UUIDHyphenated(),
			},
			{
				"id": faker.UUIDHyphenated(),
				"a": map[string]any{
					"b": faker.UUIDHyphenated(),
				},
				"c": faker.UUIDHyphenated(),
				"d": faker.UUIDHyphenated(),
			},
			{
				"id": faker.UUIDHyphenated(),
				"a": map[string]any{
					"b": faker.UUIDHyphenated(),
				},
				"c": faker.UUIDHyphenated(),
			},
		}

		_ = iv.insertMany(docs)

		ids, err := iv.findMany(
			Where("id").EQ(docs[0]["id"]).
				And(Where("a.b").LT(0)).
				Or(Where("c").EQ(docs[1]["c"])),
		)
		assert.NoError(t, err)
		assert.Len(t, ids, 2)
		assert.Contains(t, ids, docs[0]["id"])
		assert.Contains(t, ids, docs[1]["id"])
	})

	t.Run("error: nil, unique: false, nested: false", func(t *testing.T) {
		iv := newIndexView()

		iv.Create(IndexModel{
			Keys:    []string{"c", "a.b"},
			Name:    "c_a.b",
			Unique:  true,
			Partial: nil,
		})
		iv.Create(IndexModel{
			Keys:    []string{"d"},
			Name:    "d",
			Unique:  false,
			Partial: nil,
		})

		docs := []map[string]any{
			{
				"id": faker.UUIDHyphenated(),
				"a": map[string]any{
					"b": faker.UUIDHyphenated(),
				},
				"c": faker.UUIDHyphenated(),
				"d": faker.UUIDHyphenated(),
			},
			{
				"id": faker.UUIDHyphenated(),
				"a": map[string]any{
					"b": faker.UUIDHyphenated(),
				},
				"c": faker.UUIDHyphenated(),
				"d": faker.UUIDHyphenated(),
			},
			{
				"id": faker.UUIDHyphenated(),
				"a": map[string]any{
					"b": faker.UUIDHyphenated(),
				},
				"c": faker.UUIDHyphenated(),
			},
		}

		_ = iv.insertMany(docs)

		ids, err := iv.findMany(
			Where("d").EQ(docs[0]["d"]),
		)
		assert.NoError(t, err)
		assert.Len(t, ids, 1)
		assert.Contains(t, ids, docs[0]["id"])
	})

	t.Run("error: nil, unique: false, nested: true", func(t *testing.T) {
		iv := newIndexView()

		iv.Create(IndexModel{
			Keys:    []string{"c", "a.b"},
			Name:    "c_a.b",
			Unique:  true,
			Partial: nil,
		})
		iv.Create(IndexModel{
			Keys:    []string{"d", "c"},
			Name:    "d_c",
			Unique:  false,
			Partial: nil,
		})

		docs := []map[string]any{
			{
				"id": faker.UUIDHyphenated(),
				"a": map[string]any{
					"b": faker.UUIDHyphenated(),
				},
				"c": faker.UUIDHyphenated(),
				"d": faker.UUIDHyphenated(),
			},
			{
				"id": faker.UUIDHyphenated(),
				"a": map[string]any{
					"b": faker.UUIDHyphenated(),
				},
				"c": faker.UUIDHyphenated(),
				"d": faker.UUIDHyphenated(),
			},
			{
				"id": faker.UUIDHyphenated(),
				"a": map[string]any{
					"b": faker.UUIDHyphenated(),
				},
				"c": faker.UUIDHyphenated(),
			},
		}

		_ = iv.insertMany(docs)

		ids, err := iv.findMany(
			Where("d").EQ(docs[0]["d"]),
		)
		assert.NoError(t, err)
		assert.Len(t, ids, 1)
		assert.Contains(t, ids, docs[0]["id"])
	})

	t.Run("error: ErrIndexNotFound", func(t *testing.T) {
		iv := newIndexView()

		iv.Create(IndexModel{
			Keys:    []string{"a.b", "c"},
			Name:    "a.b_c",
			Unique:  true,
			Partial: nil,
		})
		iv.Create(IndexModel{
			Keys:    []string{"d"},
			Name:    "d",
			Unique:  false,
			Partial: nil,
		})

		docs := []map[string]any{
			{
				"id": faker.UUIDHyphenated(),
				"a": map[string]any{
					"b": faker.UUIDHyphenated(),
				},
				"c": faker.UUIDHyphenated(),
				"d": faker.UUIDHyphenated(),
			},
			{
				"id": faker.UUIDHyphenated(),
				"a": map[string]any{
					"b": faker.UUIDHyphenated(),
				},
				"c": faker.UUIDHyphenated(),
				"d": faker.UUIDHyphenated(),
			},
			{
				"id": faker.UUIDHyphenated(),
				"a": map[string]any{
					"b": faker.UUIDHyphenated(),
				},
				"c": faker.UUIDHyphenated(),
			},
		}

		_ = iv.insertMany(docs)

		_, err := iv.findMany(
			Where("id").EQ(docs[0]["id"]).
				Or(Where("c").EQ(docs[1]["c"])),
		)
		assert.ErrorIs(t, err, ErrIndexNotFound)
	})
}
