package memdb

import "github.com/siyul-park/memdb/internal/util/reflectutil"

type (
	Sort struct {
		Key   string
		Order Order
	}
)

func parseSorts(sorts []Sort) func(i, j map[string]any) bool {
	return func(i, j map[string]any) bool {
		for _, s := range sorts {
			x, _ := reflectutil.Get[any](i, s.Key)
			y, _ := reflectutil.Get[any](j, s.Key)

			e := reflectutil.Compare(x, y)
			if e == 0 {
				continue
			}

			if s.Order == OrderDESC {
				return e > 0
			}
			return e < 0
		}
		return false
	}
}
