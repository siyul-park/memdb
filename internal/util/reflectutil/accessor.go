package reflectutil

import (
	"github.com/iancoleman/strcase"
	"github.com/siyul-park/memdb/internal/util"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

var (
	numberSubPath = regexp.MustCompile(`\[([0-9]+)\]`)
)

func Get[T any](value any, key string) (T, bool) {
	if his, ok := get(reflect.ValueOf(value), parseKey(key)); ok {
		if v, ok := his[0].Interface().(T); ok {
			return v, true
		}
	}
	var zero T
	return zero, false
}

func Set(source any, key string, value any) bool {
	ok := set(reflect.ValueOf(source), parseKey(key), reflect.ValueOf(value))
	return ok
}

func parseKey(key string) []string {
	key = numberSubPath.ReplaceAllString(key, ".$1")
	return strings.Split(key, ".")
}

func set(source reflect.Value, path []string, value reflect.Value) bool {
	current := path[len(path)-1]

	parent := source
	if len(path) > 1 {
		parentPath := path[:len(path)-1]
		if his, ok := get(source, parentPath); ok {
			parent = his[0]
		} else {
			return false
		}
	}

	parent = rawValue(parent)
	parentType := parent.Type()
	parentKind := basicKind(parent)

	if parentType.Implements(anyType) {
		call := func(reflectMethod reflect.Method, args []reflect.Value) bool {
			numIn := reflectMethod.Type.NumIn()
			if numIn == len(args) {
				for i := 0; i < numIn; i++ {
					if !args[i].Type().AssignableTo(reflectMethod.Type.In(i)) {
						return false
					}
				}
				out := reflectMethod.Func.Call(args)
				if len(out) == 0 {
					return true
				} else if len(out) > 0 {
					okOrErr := out[len(out)-1].Interface()
					if ok, subOk := okOrErr.(bool); subOk && ok {
						return true
					} else if err, subOk := okOrErr.(error); subOk && util.IsNil(err) {
						return true
					}
				}
			}
			return false
		}

		for i := 0; i < parentType.NumMethod(); i++ {
			reflectMethod := parentType.Method(i)
			if !reflectMethod.IsExported() {
				continue
			}
			if strcase.ToLowerCamel(reflectMethod.Name) == "set"+strcase.ToCamel(current) {
				if ok := call(reflectMethod, []reflect.Value{parent, value}); ok {
					return true
				}
			}
		}
		for i := 0; i < parentType.NumMethod(); i++ {
			reflectMethod := parentType.Method(i)
			if !reflectMethod.IsExported() {
				continue
			}
			name := strcase.ToLowerCamel(reflectMethod.Name)
			if name == "set" || name == "store" || name == "save" || name == "put" {
				if ok := call(reflectMethod, []reflect.Value{parent, reflect.ValueOf(current), value}); ok {
					return true
				}
			}
		}
	}

	if parentKind == pointerKind {
		parent = parent.Elem()
		parentType = parent.Type()
		parentKind = basicKind(parent)
	}

	switch parentKind {
	case structKind:
		for i := 0; i < parentType.NumField(); i++ {
			reflectField := parentType.Field(i)
			if reflectField.IsExported() && strcase.ToLowerCamel(reflectField.Name) == current {
				field := parent.Field(i)
				if field.IsValid() && field.CanSet() {
					field.Set(value)
					return true
				}
			}
		}
	case iterableKind:
		index, err := strconv.Atoi(current)
		if err != nil {
			return false
		}
		if index >= parent.Len() {
			return false
		}
		parent.Index(index).Set(value)
		return true
	case mapKind:
		parent.SetMapIndex(reflect.ValueOf(current), value)
		return true
	}

	return false
}

func get(source reflect.Value, path []string) ([]reflect.Value, bool) {
	if len(path) == 0 {
		return []reflect.Value{source}, true
	}

	current := path[0]
	var remain []string
	if len(path) > 1 {
		remain = path[1:]
	}

	originSource := source
	source = rawValue(source)
	sourceType := source.Type()
	sourceKind := basicKind(source)

	resolve := func(result reflect.Value) ([]reflect.Value, bool) {
		his, ok := get(result, remain)
		if !ok {
			return nil, false
		}
		return append(his, originSource), true
	}

	if sourceType.Implements(anyType) {
		call := func(reflectMethod reflect.Method, args []reflect.Value) (reflect.Value, bool) {
			numIn := reflectMethod.Type.NumIn()
			numOut := reflectMethod.Type.NumOut()
			if numIn == len(args) && numOut > 0 {
				for i := 0; i < numIn; i++ {
					if !args[i].Type().ConvertibleTo(reflectMethod.Type.In(i)) {
						return reflect.Value{}, false
					}
				}
				out := reflectMethod.Func.Call(args)
				if len(out) == 1 {
					return out[0], true
				}

				okOrErr := out[len(out)-1].Interface()
				var result reflect.Value
				if len(out) == 2 {
					result = out[0]
				} else {
					var results []any
					for i := 0; i < len(out)-1; i++ {
						results = append(results, out[i].Interface())
					}
					result = reflect.ValueOf(results)
				}

				if ok, subOk := okOrErr.(bool); subOk && ok {
					return result, true
				} else if err, subOk := okOrErr.(error); subOk && util.IsNil(err) {
					return result, true
				}
			}
			return reflect.Value{}, false
		}

		for i := 0; i < sourceType.NumMethod(); i++ {
			reflectMethod := sourceType.Method(i)
			if !reflectMethod.IsExported() {
				continue
			}
			if strcase.ToLowerCamel(reflectMethod.Name) == current {
				if r, ok := call(reflectMethod, []reflect.Value{source}); ok {
					return resolve(r)
				}
			}
		}
		for i := 0; i < sourceType.NumMethod(); i++ {
			reflectMethod := sourceType.Method(i)
			if !reflectMethod.IsExported() {
				continue
			}
			name := strcase.ToLowerCamel(reflectMethod.Name)
			if name == "get" || name == "load" {
				if r, ok := call(reflectMethod, []reflect.Value{source, reflect.ValueOf(current)}); ok {
					return resolve(r)
				}
			}
		}
	}

	switch sourceKind {
	case structKind:
		for i := 0; i < sourceType.NumField(); i++ {
			reflectField := sourceType.Field(i)
			if reflectField.IsExported() && strcase.ToLowerCamel(reflectField.Name) == current {
				v := source.FieldByName(reflectField.Name)
				return resolve(v)
			}
		}
	case iterableKind:
		index, err := strconv.Atoi(current)
		if err != nil || index >= source.Len() {
			return nil, false
		}
		v := source.Index(index)
		return resolve(v)
	case mapKind:
		for _, k := range source.MapKeys() {
			if k.Interface() == current {
				v := source.MapIndex(k)
				return resolve(v)
			}
		}
	case pointerKind:
		if his, ok := get(source.Elem(), path); ok {
			his[len(his)-1] = source
			return his, true
		}
	}

	return nil, false
}
