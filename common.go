package hive

// contains common functionality shared between the encoder and decoder

// encoders and decoders can decode and encode any golang type except for chan, func and complex64/128

import (
	"reflect"
	"sort"
	"sync"
)

// Nil is how Hive represents nil value
var Nil = []byte{'\\', 'N'}

// hiveError is an error wrapper type for internal use only.
// Panics with errors are wrapped in hiveError so that the top-level recover
// can distinguish intentional panics from this package.
type hiveError struct{ error }

// An UnsupportedTypeError is returned by Marshal/Unmarshal when attempting
// to encode/decode an unsupported value type.
type UnsupportedTypeError struct {
	Type reflect.Type
}

func (e UnsupportedTypeError) Error() string {
	return "unsupported type: " + e.Type.String()
}

// field is used to encode and decode struct
type field struct {
	name       string
	index      []int
	typ        reflect.Type
	complexity int
	encoder    encoderFunc
	decoder    decoderFunc
}

// find the nested struct field by following f.index.
func (f field) findNested(v reflect.Value) (reflect.Value, bool) {
	fv := v
	for _, i := range f.index {
		if fv.Kind() == reflect.Ptr {
			if fv.IsNil() {
				return fv, false
			}
			fv = fv.Elem()
		}
		fv = fv.Field(i)
	}
	return fv, true
}

var fieldsCache sync.Map // map[reflect.Type][]field

// cachedTypeFields is like typeFields but uses a cache to avoid repeated work.
func cachedTypeFields(t reflect.Type) []field {
	if f, ok := fieldsCache.Load(t); ok {
		return f.([]field)
	}
	f, _ := fieldsCache.LoadOrStore(t, newTypeFields(t))
	return f.([]field)
}

// compute fields for given type. type should be a struct
func newTypeFields(t reflect.Type) []field {
	// Anonymous fields to explore at the current level and the next.
	var current []field
	next := []field{{typ: t}}

	// Types already visited at an earlier level.
	visited := map[reflect.Type]bool{}

	// Fields found.
	var fields []field

	for len(next) > 0 {
		current, next = next, current[:0]

		for _, f := range current {
			if visited[f.typ] {
				continue
			}
			visited[f.typ] = true

			// Scan f.typ for fields to include.
			for i := 0; i < f.typ.NumField(); i++ {
				sf := f.typ.Field(i)
				if sf.PkgPath != "" {
					continue // ignore all unexported fields
				}

				index := make([]int, len(f.index)+1)
				copy(index, f.index)
				index[len(f.index)] = i

				ft := sf.Type
				field := field{
					name:       sf.Name,
					index:      index,
					typ:        ft,
					complexity: cachedComplexity(ft),
					encoder:    typeEncoder(ft),
					decoder:    typeDecoder(ft),
				}

				if sf.Anonymous && ft.Kind() == reflect.Struct {
					// Record new anonymous struct to explore in next round.
					next = append(next, field)
					continue
				}

				fields = append(fields, field)
			}
		}
	}

	sort.Sort(byIndex(fields))

	return fields
}

// byIndex sorts field by index sequence.
type byIndex []field

func (x byIndex) Len() int { return len(x) }

func (x byIndex) Swap(i, j int) { x[i], x[j] = x[j], x[i] }

func (x byIndex) Less(i, j int) bool {
	for k, xik := range x[i].index {
		if k >= len(x[j].index) {
			return false
		}
		if xik != x[j].index[k] {
			return xik < x[j].index[k]
		}
	}
	return len(x[i].index) < len(x[j].index)
}

// slicer contains slices of data []byte
type slicer struct {
	data []byte
	idxs []int
}

// newSlicer creates a slicer on given data, where each slice is delimited by the given delimiter
func newSlicer(data []byte, delimiter byte) slicer {
	idxs := []int{-1}
	for idx, token := range data {
		if token == delimiter {
			idxs = append(idxs, idx)
		}
	}
	idxs = append(idxs, len(data))
	return slicer{data, idxs}
}

// numSlices returns number of slices the slicer holds
func (s slicer) numSlices() int {
	if len(s.data) == 0 {
		return 0
	}
	return len(s.idxs) - 1
}

// slice return idx-th slice
// if not 0 <= offset+length < s.numSlices(): call might panic
func (s slicer) slice(offset, length int) []byte {
	return s.data[s.idxs[offset]+1 : s.idxs[offset+length]]
}

var complexityMap sync.Map // map[reflect.Type]int

// cachedComplexity is like complexity but uses a cache to avoid repeated work.
func cachedComplexity(t reflect.Type) int {
	if c, ok := complexityMap.Load(t); ok {
		return c.(int)
	}
	c, _ := complexityMap.LoadOrStore(t, complexity(t))
	return c.(int)
}

// complexity(!struct) = 0
// complexity(struct) = sum(complexity(field)+1 for each field) - 1
func complexity(t reflect.Type) int {
	t = indirect(t)
	if t.Kind() != reflect.Struct {
		return 0
	}
	c := 0
	for i, n := 0, t.NumField(); i < n; i++ {
		f := t.Field(i)
		if f.PkgPath != "" {
			continue // not exported
		}
		c += cachedComplexity(indirect(f.Type)) + 1
	}
	return c - 1
}

// indirect a pointer type to the actual type
// if t.Kind() is not a pointer, returns t
// if it's a pointer, returns indirect(t.Elem())
func indirect(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t
}
