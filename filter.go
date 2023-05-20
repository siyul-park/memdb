package memdb

import (
	"encoding/json"
	"fmt"
	"github.com/siyul-park/memdb/internal/util"
	"github.com/siyul-park/memdb/internal/util/reflectutil"
	"strings"
)

type (
	Filter struct {
		OP    operator
		Key   string
		Value any
	}
	operator int

	filterHelper struct {
		key string
	}
)

const (
	EQ operator = iota
	NE
	LT
	LTE
	GT
	GTE
	IN
	NIN
	NULL
	NNULL
	AND
	OR
)

var (
	opToStr = []string{
		"=",
		"!=",
		"<",
		"<=",
		">",
		">=",
		"IN",
		"NOT IN",
		"IS NULL",
		"IS NOT NULL",
		"AND",
		"OR",
	}
)

func Where(key string) *filterHelper {
	return &filterHelper{
		key: key,
	}
}

func (fh *filterHelper) EQ(value any) *Filter {
	return &Filter{
		OP:    EQ,
		Key:   fh.key,
		Value: value,
	}
}

func (fh *filterHelper) NE(value any) *Filter {
	return &Filter{
		OP:    NE,
		Key:   fh.key,
		Value: value,
	}
}

func (fh *filterHelper) LT(value any) *Filter {
	return &Filter{
		Key:   fh.key,
		OP:    LT,
		Value: value,
	}
}

func (fh *filterHelper) LTE(value any) *Filter {
	return &Filter{
		OP:    LTE,
		Key:   fh.key,
		Value: value,
	}
}

func (fh *filterHelper) GT(value any) *Filter {
	return &Filter{
		OP:    GT,
		Key:   fh.key,
		Value: value,
	}
}

func (fh *filterHelper) GTE(value any) *Filter {
	return &Filter{
		OP:    GTE,
		Key:   fh.key,
		Value: value,
	}
}

func (fh *filterHelper) IN(slice ...any) *Filter {
	return &Filter{
		OP:    IN,
		Key:   fh.key,
		Value: slice,
	}
}

func (fh *filterHelper) NotIN(slice ...any) *Filter {
	return &Filter{
		OP:    NIN,
		Key:   fh.key,
		Value: slice,
	}
}

func (fh *filterHelper) IsNull() *Filter {
	return &Filter{
		OP:  NULL,
		Key: fh.key,
	}
}

func (fh *filterHelper) IsNotNull() *Filter {
	return &Filter{
		OP:  NNULL,
		Key: fh.key,
	}
}

func (ft *Filter) And(x ...*Filter) *Filter {
	var v []*Filter
	for _, e := range append([]*Filter{ft}, x...) {
		if !util.IsNil(e) {
			v = append(v, e)
		}
	}

	return &Filter{
		OP:    AND,
		Value: v,
	}
}

func (ft *Filter) Or(x ...*Filter) *Filter {
	var v []*Filter
	for _, e := range append([]*Filter{ft}, x...) {
		if !util.IsNil(e) {
			v = append(v, e)
		}
	}

	return &Filter{
		OP:    OR,
		Value: v,
	}
}

func (ft *Filter) String() (string, error) {
	if ft.OP == AND || ft.OP == OR {
		var parsed []string
		if value, ok := ft.Value.([]*Filter); ok {
			for _, v := range value {
				c, e := v.String()
				if e != nil {
					return "", e
				}
				parsed = append(parsed, "("+c+")")
			}
		}
		return strings.Join(parsed, " "+opToStr[ft.OP]+" "), nil
	}
	if ft.OP == NULL || ft.OP == NNULL {
		return ft.Key + " " + opToStr[ft.OP], nil
	}

	b, err := json.Marshal(ft.Value)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s %s %s", ft.Key, opToStr[ft.OP], string(b)), nil
}

func parseFilter(filter *Filter) func(map[string]any) bool {
	if util.IsNil(filter) {
		return func(_ map[string]any) bool {
			return true
		}
	}

	switch filter.OP {
	case EQ:
		return func(m map[string]any) bool {
			if v, ok := reflectutil.Get[any](m, filter.Key); !ok {
				return false
			} else {
				return reflectutil.Equal(v, filter.Value)
			}
		}
	case NE:
		return func(m map[string]any) bool {
			if v, ok := reflectutil.Get[any](m, filter.Key); !ok {
				return false
			} else {
				return !reflectutil.Equal(v, filter.Value)
			}
		}
	case LT:
		return func(m map[string]any) bool {
			if v, ok := reflectutil.Get[any](m, filter.Key); !ok {
				return false
			} else {
				return reflectutil.Compare(v, filter.Value) < 0
			}
		}
	case LTE:
		return func(m map[string]any) bool {
			if v, ok := reflectutil.Get[any](m, filter.Key); !ok {
				return false
			} else {
				return reflectutil.Compare(v, filter.Value) <= 0
			}
		}
	case GT:
		return func(m map[string]any) bool {
			if v, ok := reflectutil.Get[any](m, filter.Key); !ok {
				return false
			} else {
				return reflectutil.Compare(v, filter.Value) > 0
			}
		}
	case GTE:
		return func(m map[string]any) bool {
			if v, ok := reflectutil.Get[any](m, filter.Key); !ok {
				return false
			} else {
				return reflectutil.Compare(v, filter.Value) >= 0
			}
		}
	case IN:
		return func(m map[string]any) bool {
			if v, ok := reflectutil.Get[any](m, filter.Key); !ok {
				return false
			} else if children, ok := filter.Value.([]any); !ok {
				return false
			} else {
				for _, child := range children {
					if reflectutil.Equal(v, child) {
						return true
					}
				}
				return false
			}
		}
	case NIN:
		return func(m map[string]any) bool {
			if v, ok := reflectutil.Get[any](m, filter.Key); !ok {
				return false
			} else if children, ok := filter.Value.([]any); !ok {
				return false
			} else {
				for _, child := range children {
					if reflectutil.Equal(v, child) {
						return false
					}
				}
				return true
			}
		}
	case NULL:
		return func(m map[string]any) bool {
			if v, ok := reflectutil.Get[any](m, filter.Key); !ok {
				return true
			} else {
				return util.IsNil(v)
			}
		}
	case NNULL:
		return func(m map[string]any) bool {
			if v, ok := reflectutil.Get[any](m, filter.Key); !ok {
				return false
			} else {
				return !util.IsNil(v)
			}
		}
	case AND:
		if children, ok := filter.Value.([]*Filter); !ok {
			return func(m map[string]any) bool {
				return false
			}
		} else {
			var parsed []func(map[string]any) bool
			for _, child := range children {
				parsed = append(parsed, parseFilter(child))
			}
			return func(m map[string]any) bool {
				for _, p := range parsed {
					if !p(m) {
						return false
					}
				}
				return true
			}
		}
	case OR:
		if children, ok := filter.Value.([]*Filter); !ok {
			return func(m map[string]any) bool {
				return false
			}
		} else {
			var parsed []func(map[string]any) bool
			for _, child := range children {
				parsed = append(parsed, parseFilter(child))
			}
			return func(m map[string]any) bool {
				for _, p := range parsed {
					if p(m) {
						return true
					}
				}
				return false
			}
		}
	}

	return func(_ map[string]any) bool {
		return false
	}
}

func filterToExample(filter *Filter) ([]map[string]any, bool) {
	if util.IsNil(filter) {
		return nil, false
	}

	switch filter.OP {
	case EQ:
		return []map[string]any{{filter.Key: filter.Value}}, true
	case NE:
		return nil, false
	case LT:
		return nil, false
	case LTE:
		return nil, false
	case GT:
		return nil, false
	case GTE:
		return nil, false
	case IN:
		if children, ok := filter.Value.([]any); !ok {
			return nil, false
		} else {
			var examples []map[string]any
			for _, child := range children {
				examples = append(examples, map[string]any{filter.Key: child})
			}
			return examples, true
		}
	case NIN:
		return nil, false
	case NULL:
		return []map[string]any{{filter.Key: nil}}, true
	case NNULL:
		return nil, false
	case AND:
		if children, ok := filter.Value.([]*Filter); !ok {
			return nil, false
		} else {
			example := map[string]any{}
			for _, child := range children {
				e, _ := filterToExample(child)
				if len(e) == 0 {
				} else if len(e) == 1 {
					for k, v := range e[0] {
						if _, ok := example[k]; ok {
							return nil, true
						} else {
							example[k] = v
						}
					}
				} else {
					return nil, false
				}
			}
			return []map[string]any{example}, true
		}
	case OR:
		if children, ok := filter.Value.([]*Filter); !ok {
			return nil, false
		} else {
			var examples []map[string]any
			for _, child := range children {
				if e, ok := filterToExample(child); ok {
					examples = append(examples, e...)
				} else {
					return nil, false
				}
			}
			return examples, true
		}
	}

	return nil, false
}
