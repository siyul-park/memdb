package memdb

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseSorts(t *testing.T) {
	testCases := []struct {
		when   []Sort
		whenX  map[string]any
		whenY  map[string]any
		expect bool
	}{
		{
			when:   []Sort{{Key: "1", Order: OrderASC}},
			whenX:  map[string]any{"1": "1"},
			whenY:  map[string]any{"1": "2"},
			expect: true,
		},
		{
			when:   []Sort{{Key: "1", Order: OrderDESC}},
			whenX:  map[string]any{"1": "1"},
			whenY:  map[string]any{"1": "2"},
			expect: false,
		},
	}

	for _, tc := range testCases {
		compare := parseSorts(tc.when)
		assert.Equal(t, tc.expect, compare(tc.whenX, tc.whenY))
	}
}
