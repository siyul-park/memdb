package memdb

import (
	"github.com/go-faker/faker/v4"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWhere(t *testing.T) {
	f := faker.UUIDHyphenated()
	wh := Where(f)
	assert.Equal(t, &filterHelper{key: f}, wh)
}

func TestFilterHelper_EQ(t *testing.T) {
	f := faker.UUIDHyphenated()
	v := faker.UUIDHyphenated()

	wh := Where(f)

	assert.Equal(t, &Filter{
		Key:   f,
		OP:    EQ,
		Value: v,
	}, wh.EQ(v))
}

func TestFilterHelper_NE(t *testing.T) {
	f := faker.UUIDHyphenated()
	v := faker.UUIDHyphenated()

	wh := Where(f)

	assert.Equal(t, &Filter{
		Key:   f,
		OP:    NE,
		Value: v,
	}, wh.NE(v))
}

func TestFilterHelper_LT(t *testing.T) {
	f := faker.UUIDHyphenated()
	v := faker.UUIDHyphenated()

	wh := Where(f)

	assert.Equal(t, &Filter{
		Key:   f,
		OP:    LT,
		Value: v,
	}, wh.LT(v))
}

func TestFilterHelper_LTE(t *testing.T) {
	f := faker.UUIDHyphenated()
	v := faker.UUIDHyphenated()

	wh := Where(f)

	assert.Equal(t, &Filter{
		Key:   f,
		OP:    LTE,
		Value: v,
	}, wh.LTE(v))
}

func TestFilterHelper_GT(t *testing.T) {
	f := faker.UUIDHyphenated()
	v := faker.UUIDHyphenated()

	wh := Where(f)

	assert.Equal(t, &Filter{
		Key:   f,
		OP:    GT,
		Value: v,
	}, wh.GT(v))
}

func TestFilterHelper_GTE(t *testing.T) {
	f := faker.UUIDHyphenated()
	v := faker.UUIDHyphenated()

	wh := Where(f)

	assert.Equal(t, &Filter{
		Key:   f,
		OP:    GTE,
		Value: v,
	}, wh.GTE(v))
}

func TestFilterHelper_IN(t *testing.T) {
	f := faker.UUIDHyphenated()
	v := faker.UUIDHyphenated()

	wh := Where(f)

	assert.Equal(t, &Filter{
		Key:   f,
		OP:    IN,
		Value: []any{v},
	}, wh.IN(v))
}

func TestFilterHelper_NotIN(t *testing.T) {
	f := faker.UUIDHyphenated()
	v := faker.UUIDHyphenated()

	wh := Where(f)

	assert.Equal(t, &Filter{
		Key:   f,
		OP:    NIN,
		Value: []any{v},
	}, wh.NotIN(v))
}

func TestFilterHelper_IsNull(t *testing.T) {
	f := faker.UUIDHyphenated()

	wh := Where(f)

	assert.Equal(t, &Filter{
		Key: f,
		OP:  NULL,
	}, wh.IsNull())
}

func TestFilterHelper_IsNotNull(t *testing.T) {
	f := faker.UUIDHyphenated()

	wh := Where(f)

	assert.Equal(t, &Filter{
		Key: f,
		OP:  NNULL,
	}, wh.IsNotNull())
}

func TestFilter_And(t *testing.T) {
	f1 := faker.UUIDHyphenated()
	f2 := faker.UUIDHyphenated()
	v1 := faker.UUIDHyphenated()
	v2 := faker.UUIDHyphenated()

	q1 := Where(f1).EQ(v1)
	q2 := Where(f2).EQ(v2)

	q := q1.And(q2)

	assert.Equal(t, &Filter{
		OP:    AND,
		Value: []*Filter{q1, q2},
	}, q)
}

func TestFilter_Or(t *testing.T) {
	f1 := faker.UUIDHyphenated()
	f2 := faker.UUIDHyphenated()
	v1 := faker.UUIDHyphenated()
	v2 := faker.UUIDHyphenated()

	q1 := Where(f1).EQ(v1)
	q2 := Where(f2).EQ(v2)

	q := q1.Or(q2)

	assert.Equal(t, &Filter{
		OP:    OR,
		Value: []*Filter{q1, q2},
	}, q)
}

func TestFilter_String(t *testing.T) {
	testCases := []struct {
		when   *Filter
		expect string
	}{
		{
			when:   Where("1").EQ("1"),
			expect: "1 = \"1\"",
		},
		{
			when:   Where("1").EQ(1),
			expect: "1 = 1",
		},
		{
			when:   Where("1").EQ(true),
			expect: "1 = true",
		},
		{
			when:   Where("1").EQ(nil),
			expect: "1 = null",
		},

		{
			when:   Where("1").NE("1"),
			expect: "1 != \"1\"",
		},
		{
			when:   Where("1").NE(1),
			expect: "1 != 1",
		},
		{
			when:   Where("1").NE(true),
			expect: "1 != true",
		},
		{
			when:   Where("1").NE(nil),
			expect: "1 != null",
		},

		{
			when:   Where("1").LT("1"),
			expect: "1 < \"1\"",
		},
		{
			when:   Where("1").LT(1),
			expect: "1 < 1",
		},

		{
			when:   Where("1").LTE("1"),
			expect: "1 <= \"1\"",
		},
		{
			when:   Where("1").LTE(1),
			expect: "1 <= 1",
		},

		{
			when:   Where("1").GT("1"),
			expect: "1 > \"1\"",
		},
		{
			when:   Where("1").GT(1),
			expect: "1 > 1",
		},

		{
			when:   Where("1").GTE("1"),
			expect: "1 >= \"1\"",
		},
		{
			when:   Where("1").GTE(1),
			expect: "1 >= 1",
		},

		{
			when:   Where("1").IN("1"),
			expect: "1 IN [\"1\"]",
		},
		{
			when:   Where("1").IN(1),
			expect: "1 IN [1]",
		},

		{
			when:   Where("1").NotIN("1"),
			expect: "1 NOT IN [\"1\"]",
		},
		{
			when:   Where("1").NotIN(1),
			expect: "1 NOT IN [1]",
		},

		{
			when:   Where("1").IsNull(),
			expect: "1 IS NULL",
		},
		{
			when:   Where("1").IsNotNull(),
			expect: "1 IS NOT NULL",
		},

		{
			when:   Where("1").EQ(1).And(Where("2").EQ(2)),
			expect: "(1 = 1) AND (2 = 2)",
		},
		{
			when:   Where("1").EQ(1).And(Where("2").EQ(2)).And(Where("3").EQ(3)),
			expect: "((1 = 1) AND (2 = 2)) AND (3 = 3)",
		},

		{
			when:   Where("1").EQ(1).Or(Where("2").EQ(2)),
			expect: "(1 = 1) OR (2 = 2)",
		},
		{
			when:   Where("1").EQ(1).Or(Where("2").EQ(2)).Or(Where("3").EQ(3)),
			expect: "((1 = 1) OR (2 = 2)) OR (3 = 3)",
		},

		{
			when:   Where("1").EQ(1).And(Where("2").EQ(2)).Or(Where("3").EQ(3)),
			expect: "((1 = 1) AND (2 = 2)) OR (3 = 3)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.expect, func(t *testing.T) {
			c, err := tc.when.String()
			assert.NoError(t, err)
			assert.Equal(t, tc.expect, c)
		})
	}
}

func TestParseFilter(t *testing.T) {
	testCases := []struct {
		whenFilter *Filter
		whenValue  map[string]any
		expect     bool
	}{
		{
			whenFilter: Where("a").EQ("1"),
			whenValue: map[string]any{
				"a": "1",
			},
			expect: true,
		},
		{
			whenFilter: Where("a").EQ("1"),
			whenValue: map[string]any{
				"a": "2",
			},
			expect: false,
		},
		{
			whenFilter: Where("a.b").EQ("1"),
			whenValue: map[string]any{
				"a": map[string]any{
					"b": "1",
				},
			},
			expect: true,
		},
		{
			whenFilter: Where("a.b").EQ("2"),
			whenValue: map[string]any{
				"a": map[string]any{
					"b": "1",
				},
			},
			expect: false,
		},
		{
			whenFilter: Where("b").EQ("1"),
			whenValue: map[string]any{
				"a": "1",
			},
			expect: false,
		},

		{
			whenFilter: Where("a").NE("1"),
			whenValue: map[string]any{
				"a": "1",
			},
			expect: false,
		},
		{
			whenFilter: Where("a").NE("1"),
			whenValue: map[string]any{
				"a": "2",
			},
			expect: true,
		},
		{
			whenFilter: Where("a.b").NE("1"),
			whenValue: map[string]any{
				"a": map[string]any{
					"b": "1",
				},
			},
			expect: false,
		},
		{
			whenFilter: Where("a.b").NE("2"),
			whenValue: map[string]any{
				"a": map[string]any{
					"b": "1",
				},
			},
			expect: true,
		},
		{
			whenFilter: Where("b").NE("1"),
			whenValue: map[string]any{
				"a": "1",
			},
			expect: false,
		},

		{
			whenFilter: Where("a").LT(1),
			whenValue: map[string]any{
				"a": 0,
			},
			expect: true,
		},
		{
			whenFilter: Where("a").LT(1),
			whenValue: map[string]any{
				"a": 1,
			},
			expect: false,
		},
		{
			whenFilter: Where("a.b").LT(1),
			whenValue: map[string]any{
				"a": map[string]any{
					"b": 0,
				},
			},
			expect: true,
		},
		{
			whenFilter: Where("a.b").LT(1),
			whenValue: map[string]any{
				"a": map[string]any{
					"b": 1,
				},
			},
			expect: false,
		},
		{
			whenFilter: Where("b").LT(1),
			whenValue: map[string]any{
				"a": 1,
			},
			expect: false,
		},

		{
			whenFilter: Where("a").LTE(1),
			whenValue: map[string]any{
				"a": 1,
			},
			expect: true,
		},
		{
			whenFilter: Where("a").LTE(1),
			whenValue: map[string]any{
				"a": 2,
			},
			expect: false,
		},
		{
			whenFilter: Where("a.b").LTE(1),
			whenValue: map[string]any{
				"a": map[string]any{
					"b": 1,
				},
			},
			expect: true,
		},
		{
			whenFilter: Where("a.b").LTE(1),
			whenValue: map[string]any{
				"a": map[string]any{
					"b": 2,
				},
			},
			expect: false,
		},
		{
			whenFilter: Where("b").LTE(1),
			whenValue: map[string]any{
				"a": 1,
			},
			expect: false,
		},

		{
			whenFilter: Where("a").GT(1),
			whenValue: map[string]any{
				"a": 2,
			},
			expect: true,
		},
		{
			whenFilter: Where("a").GT(1),
			whenValue: map[string]any{
				"a": 1,
			},
			expect: false,
		},
		{
			whenFilter: Where("a.b").GT(1),
			whenValue: map[string]any{
				"a": map[string]any{
					"b": 2,
				},
			},
			expect: true,
		},
		{
			whenFilter: Where("a.b").GT(1),
			whenValue: map[string]any{
				"a": map[string]any{
					"b": 1,
				},
			},
			expect: false,
		},
		{
			whenFilter: Where("b").GT(1),
			whenValue: map[string]any{
				"a": 1,
			},
			expect: false,
		},

		{
			whenFilter: Where("a").GTE(1),
			whenValue: map[string]any{
				"a": 1,
			},
			expect: true,
		},
		{
			whenFilter: Where("a").GTE(1),
			whenValue: map[string]any{
				"a": 0,
			},
			expect: false,
		},
		{
			whenFilter: Where("a.b").GTE(1),
			whenValue: map[string]any{
				"a": map[string]any{
					"b": 1,
				},
			},
			expect: true,
		},
		{
			whenFilter: Where("a.b").GTE(1),
			whenValue: map[string]any{
				"a": map[string]any{
					"b": 0,
				},
			},
			expect: false,
		},
		{
			whenFilter: Where("b").GTE(1),
			whenValue: map[string]any{
				"a": 1,
			},
			expect: false,
		},

		{
			whenFilter: Where("a").IN(0, 1),
			whenValue: map[string]any{
				"a": 1,
			},
			expect: true,
		},
		{
			whenFilter: Where("a").IN(-1, 0),
			whenValue: map[string]any{
				"a": 1,
			},
			expect: false,
		},
		{
			whenFilter: Where("a.b").IN(0, 1),
			whenValue: map[string]any{
				"a": map[string]any{
					"b": 1,
				},
			},
			expect: true,
		},
		{
			whenFilter: Where("a.b").IN(-1, 0),
			whenValue: map[string]any{
				"a": map[string]any{
					"b": 1,
				},
			},
			expect: false,
		},
		{
			whenFilter: Where("b").IN(-1, 0),
			whenValue: map[string]any{
				"a": 1,
			},
			expect: false,
		},

		{
			whenFilter: Where("a").NotIN(-1, 0),
			whenValue: map[string]any{
				"a": 1,
			},
			expect: true,
		},
		{
			whenFilter: Where("a").NotIN(0, 1),
			whenValue: map[string]any{
				"a": 1,
			},
			expect: false,
		},
		{
			whenFilter: Where("a.b").NotIN(-1, 0),
			whenValue: map[string]any{
				"a": map[string]any{
					"b": 1,
				},
			},
			expect: true,
		},
		{
			whenFilter: Where("a.b").NotIN(0, 1),
			whenValue: map[string]any{
				"a": map[string]any{
					"b": 1,
				},
			},
			expect: false,
		},
		{
			whenFilter: Where("b").NotIN(-1, 0),
			whenValue: map[string]any{
				"a": 1,
			},
			expect: false,
		},

		{
			whenFilter: Where("a").IsNull(),
			whenValue: map[string]any{
				"a": 1,
			},
			expect: false,
		},
		{
			whenFilter: Where("a").IsNull(),
			whenValue: map[string]any{
				"a": nil,
			},
			expect: true,
		},
		{
			whenFilter: Where("a").IsNull(),
			whenValue:  map[string]any{},
			expect:     true,
		},

		{
			whenFilter: Where("a").IsNotNull(),
			whenValue: map[string]any{
				"a": 1,
			},
			expect: true,
		},
		{
			whenFilter: Where("a").IsNotNull(),
			whenValue: map[string]any{
				"a": nil,
			},
			expect: false,
		},
		{
			whenFilter: Where("a").IsNotNull(),
			whenValue:  map[string]any{},
			expect:     false,
		},

		{
			whenFilter: Where("a").EQ(1).And(Where("b").EQ(2)),
			whenValue: map[string]any{
				"a": 1,
				"b": 2,
			},
			expect: true,
		},
		{
			whenFilter: Where("a").EQ(1).And(Where("b").EQ(2)),
			whenValue: map[string]any{
				"a": 1,
				"b": 1,
			},
			expect: false,
		},

		{
			whenFilter: Where("a").EQ(1).Or(Where("b").EQ(2)),
			whenValue: map[string]any{
				"a": 0,
				"b": 2,
			},
			expect: true,
		},
		{
			whenFilter: Where("a").EQ(1).Or(Where("b").EQ(2)),
			whenValue: map[string]any{
				"a": 1,
				"b": 0,
			},
			expect: true,
		},
		{
			whenFilter: Where("a").EQ(1).Or(Where("b").EQ(2)),
			whenValue: map[string]any{
				"a": 1,
				"b": 2,
			},
			expect: true,
		},
		{
			whenFilter: Where("a").EQ(1).Or(Where("b").EQ(2)),
			whenValue: map[string]any{
				"a": 2,
				"b": 1,
			},
			expect: false,
		},

		{
			whenFilter: Where("a").EQ(1).And(Where("b").EQ(2)).Or(Where("c").EQ(3)),
			whenValue: map[string]any{
				"a": 1,
				"b": 2,
				"c": 3,
			},
			expect: true,
		},
		{
			whenFilter: Where("a").EQ(1).And(Where("b").EQ(2)).Or(Where("c").EQ(3)),
			whenValue: map[string]any{
				"a": 0,
				"b": 0,
				"c": 3,
			},
			expect: true,
		},
		{
			whenFilter: Where("a").EQ(1).And(Where("b").EQ(2)).Or(Where("c").EQ(3)),
			whenValue: map[string]any{
				"a": 1,
				"b": 2,
				"c": 0,
			},
			expect: true,
		},
		{
			whenFilter: Where("a").EQ(1).And(Where("b").EQ(2)).Or(Where("c").EQ(3)),
			whenValue: map[string]any{
				"a": 0,
				"b": 0,
				"c": 0,
			},
			expect: false,
		},

		{
			whenFilter: Where("a").EQ(true),
			whenValue: map[string]any{
				"a": true,
			},
			expect: true,
		},
		{
			whenFilter: Where("a").EQ(true),
			whenValue: map[string]any{
				"a": false,
			},
			expect: false,
		},
	}

	for _, tc := range testCases {
		query, _ := tc.whenFilter.String()
		t.Run(query, func(t *testing.T) {
			p := parseFilter(tc.whenFilter)
			assert.Equal(t, tc.expect, p(tc.whenValue))
		})
	}
}

func TestFilterToExample(t *testing.T) {
	testCases := []struct {
		whenFilter     *Filter
		expectExamples []map[string]any
		expectOK       bool
	}{
		{
			whenFilter: Where("a").EQ("1"),
			expectExamples: []map[string]any{
				{
					"a": "1",
				},
			},
			expectOK: true,
		},
		{
			whenFilter:     Where("a").NE("1"),
			expectExamples: nil,
			expectOK:       false,
		},
		{
			whenFilter:     Where("a").LT("1"),
			expectExamples: nil,
			expectOK:       false,
		},
		{
			whenFilter:     Where("a").LTE("1"),
			expectExamples: nil,
			expectOK:       false,
		},
		{
			whenFilter:     Where("a").GT("1"),
			expectExamples: nil,
			expectOK:       false,
		},
		{
			whenFilter:     Where("a").GTE("1"),
			expectExamples: nil,
			expectOK:       false,
		},
		{
			whenFilter: Where("a").IN("1"),
			expectExamples: []map[string]any{
				{
					"a": "1",
				},
			},
			expectOK: true,
		},
		{
			whenFilter:     Where("a").NotIN("1"),
			expectExamples: nil,
			expectOK:       false,
		},
		{
			whenFilter: Where("a").IsNull(),
			expectExamples: []map[string]any{
				{
					"a": nil,
				},
			},
			expectOK: true,
		},
		{
			whenFilter:     Where("a").IsNotNull(),
			expectExamples: nil,
			expectOK:       false,
		},
		{
			whenFilter: Where("a").EQ("1").
				And(Where("b").EQ("1")),
			expectExamples: []map[string]any{
				{
					"a": "1",
					"b": "1",
				},
			},
			expectOK: true,
		},
		{
			whenFilter: Where("a").EQ("1").
				Or(Where("b").EQ("1")),
			expectExamples: []map[string]any{
				{
					"a": "1",
				},
				{
					"b": "1",
				},
			},
			expectOK: true,
		},
		{
			whenFilter: Where("a").EQ("1").
				And(
					Where("b").EQ("1"),
					Where("c").GT("1"),
				),
			expectExamples: []map[string]any{
				{
					"a": "1",
					"b": "1",
				},
			},
			expectOK: true,
		},
		{
			whenFilter: Where("a").EQ("1").
				Or(
					Where("b").EQ("1"),
					Where("c").GT("1"),
				),
			expectExamples: nil,
			expectOK:       false,
		},
	}

	for _, tc := range testCases {
		query, _ := tc.whenFilter.String()
		t.Run(query, func(t *testing.T) {
			examples, ok := filterToExample(tc.whenFilter)
			assert.Equal(t, tc.expectExamples, examples)
			assert.Equal(t, tc.expectOK, ok)
		})
	}
}
