package reflectutil

import (
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

func TestGet(t *testing.T) {
	testCases := []struct {
		whenSource   any
		whenKey      string
		expectResult any
		expectOk     bool
	}{
		{
			whenSource:   map[string]any{"k1": map[string]any{"k2": 1}},
			whenKey:      "k1.k2",
			expectResult: 1,
			expectOk:     true,
		},
		{
			whenSource:   map[string]any{"k1": []map[string]any{{"k2": 1}}},
			whenKey:      "k1[0].k2",
			expectResult: 1,
			expectOk:     true,
		},
		{
			whenSource: map[string]any{"k1": func() *sync.Map {
				m := sync.Map{}
				m.Store("k2", 1)
				return &m
			}()},
			whenKey:      "k1.k2",
			expectResult: 1,
			expectOk:     true,
		},
		{
			whenSource: map[string]any{"k1": struct {
				K2 int
			}{
				K2: 1,
			}},
			whenKey:      "k1.k2",
			expectResult: 1,
			expectOk:     true,
		},
	}

	for _, tc := range testCases {
		res, ok := Get[any](tc.whenSource, tc.whenKey)
		assert.Equal(t, tc.expectOk, ok)
		if ok {
			assert.Equal(t, tc.expectResult, res)
		}
	}
}

func TestSet(t *testing.T) {
	testCases := []struct {
		whenSource any
		whenKey    string
		whenValue  any
		expectOk   bool
	}{
		{
			whenSource: map[string]any{"k1": map[string]any{"k2": 1}},
			whenKey:    "k1.k2",
			whenValue:  2,
			expectOk:   true,
		},
		{
			whenSource: map[string]any{"k1": map[string]any{}},
			whenKey:    "k1.k2",
			whenValue:  2,
			expectOk:   true,
		},
		{
			whenSource: map[string]any{"k1": &map[string]any{}},
			whenKey:    "k1.k2",
			whenValue:  2,
			expectOk:   true,
		},
		{
			whenSource: map[string]any{"k1": []map[string]any{{"k2": 1}}},
			whenKey:    "k1[0].k2",
			whenValue:  2,
			expectOk:   true,
		},
		{
			whenSource: map[string]any{"k1": []map[string]any{{"k2": 0}}},
			whenKey:    "k1[0]",
			whenValue:  map[string]any{"k2": 1},
			expectOk:   true,
		},
		{
			whenSource: map[string]any{"k1": &sync.Map{}},
			whenKey:    "k1.k2",
			whenValue:  2,
			expectOk:   true,
		},
		{
			whenSource: map[string]any{"k1": &struct {
				K2 int
			}{
				K2: 1,
			}},
			whenKey:   "k1.k2",
			whenValue: 2,
			expectOk:  true,
		},
	}

	for _, tc := range testCases {
		ok := Set(&tc.whenSource, tc.whenKey, tc.whenValue)
		assert.Equal(t, tc.expectOk, ok)
		if ok {
			res, ok := Get[any](tc.whenSource, tc.whenKey)
			assert.Equal(t, tc.expectOk, ok)
			assert.Equal(t, tc.whenValue, res)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	testCases := []struct {
		name   string
		source any
		key    string
	}{
		{
			name:   "map",
			source: map[string]any{"k1": map[string]any{"k2": 1}},
			key:    "k1.k2",
		},
		{
			name:   "slice",
			source: map[string]any{"k1": []map[string]any{{"k2": 1}}},
			key:    "k1[0].k2",
		},
		{
			name: "sync.Map",
			source: map[string]any{"k1": func() *sync.Map {
				m := sync.Map{}
				m.Store("k2", 1)
				return &m
			}()},
			key: "k1.k2",
		},
		{
			name: "struct",
			source: map[string]any{"k1": struct {
				K2 int
			}{
				K2: 1,
			}},
			key: "k1.k2",
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			_, ok := Get[any](tc.source, tc.key)
			assert.True(b, ok)
		})
	}
}

func BenchmarkSet(b *testing.B) {
	testCases := []struct {
		name   string
		source any
		key    string
		value  any
	}{
		{
			name:   "map override",
			source: map[string]any{"k1": map[string]any{"k2": 1}},
			key:    "k1.k2",
			value:  2,
		},
		{
			name:   "map",
			source: map[string]any{"k1": map[string]any{}},
			key:    "k1.k2",
			value:  2,
		},
		{
			name:   "map ptr",
			source: map[string]any{"k1": &map[string]any{}},
			key:    "k1.k2",
			value:  2,
		},
		{
			name:   "slice",
			source: map[string]any{"k1": []map[string]any{{"k2": 1}}},
			key:    "k1[0].k2",
			value:  2,
		},
		{
			name:   "slice override",
			source: map[string]any{"k1": []map[string]any{{"k2": 0}}},
			key:    "k1[0]",
			value:  map[string]any{"k2": 1},
		},
		{
			name:   "sync.Map",
			source: map[string]any{"k1": &sync.Map{}},
			key:    "k1.k2",
			value:  2,
		},
		{
			name: "struct",
			source: map[string]any{"k1": &struct {
				K2 int
			}{
				K2: 1,
			}},
			key:   "k1.k2",
			value: 2,
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			ok := Set(tc.source, tc.key, tc.value)
			assert.True(b, ok)
		})
	}
}
