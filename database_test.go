package memdb

import (
	"github.com/go-faker/faker/v4"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDatabase_Name(t *testing.T) {
	db := New(faker.Word())

	name := db.Name()
	assert.NotEmpty(t, name)
}

func TestDatabase_Collection(t *testing.T) {
	db := New(faker.Word())

	coll := db.Collection(faker.UUIDHyphenated())
	assert.NotNil(t, coll)
}

func TestDatabase_Drop(t *testing.T) {
	db := New(faker.Word())

	coll := db.Collection(faker.UUIDHyphenated())
	assert.NotNil(t, coll)

	_, err := coll.InsertOne(map[string]any{
		"id": faker.UUIDHyphenated(),
	})
	assert.NoError(t, err)

	db.Drop()

	many, err := coll.FindMany(nil)
	assert.NoError(t, err)
	assert.Len(t, many, 0)
}
