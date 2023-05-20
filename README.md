[![check](https://github.com/siyul-park/memdb/actions/workflows/check.yml/badge.svg)](https://github.com/siyul-park/memdb/actions/workflows/check.yml)
[![codecov](https://codecov.io/gh/siyul-park/memdb/branch/master/graph/badge.svg?token=ICZfrp7K5c)](https://codecov.io/gh/siyul-park/memdb)
# memdb
Provides a document-based database that operates as in-memory.

## Getting Started
### Install
```shell
go get -u github.com/siyul-park/memdb
```

### Example
```go
db := memdb.New(faker.Name())
coll := db.Collection("person")

iv := coll.Indexes()
iv.Create(memdb.IndexModel{
    Keys:    []string{"name"},
    Name:    "_name",
    Unique:  true,
    Partial: nil,
})

id, _ := coll.InsertOne(map[string]any{
    "id": faker.UUIDHyphenated(),
    "name": faker.Name()
})

it, _ := coll.FindOne(memdb.Where("id").EQ(id))

count, _ := coll.UpdateMany(memdb.Where("id").EQ(id), map[string]any{
    "name": faker.Name()
})

ok, _ := coll.DeleteOne(memdb.Where("id").EQ(id))
```

## Benchmark
```shell
cpu: Intel(R) Core(TM) i9-9880H CPU @ 2.30GHz
BenchmarkCollection_InsertOne-16           65014             17569 ns/op           11200 B/op        237 allocs/op
BenchmarkCollection_InsertMany-16           6759            187039 ns/op          112505 B/op       2370 allocs/op
BenchmarkCollection_UpdateOne-16           72742             18056 ns/op            9006 B/op        207 allocs/op
BenchmarkCollection_UpdateMany-16            759           1365635 ns/op          345795 B/op      13609 allocs/op
BenchmarkCollection_DeleteOne-16          188400              7111 ns/op            1305 B/op         44 allocs/op
BenchmarkCollection_DeleteMany-16          33098             38385 ns/op            8724 B/op        329 allocs/op
BenchmarkCollection_FindOne/with_index-16                 420176              2522 ns/op             936 B/op         27 allocs/op
BenchmarkCollection_FindOne/without_index-16                2032            642105 ns/op          160921 B/op       6534 allocs/op
BenchmarkCollection_FindMany/with_index-16                470139              2185 ns/op             776 B/op         22 allocs/op
BenchmarkCollection_FindMany/without_index-16                870           1409674 ns/op          324552 B/op      13155 allocs/op
```
