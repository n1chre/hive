package hive

import (
	"fmt"
	"reflect"
	"strconv"
	"sync"
)

// Unmarshal will decode the data into given interface. Given interface should be addressable (pointer)
// returns error on any kind of data error
func Unmarshal(data []byte, v interface{}) (err error) {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return &InvalidUnmarshalError{rv.Type()}
	}
	rv = rv.Elem()

	defer func() {
		if r := recover(); r != nil {
			if he, ok := r.(hiveError); ok {
				err = he.error
			} else {
				panic(r)
			}
		}
	}()

	dec := typeDecoder(rv.Type())
	var d decodeState
	dec(&d, data, rv)
	return nil
}

// Unmarshaler is the interface implemented by types that can unmarshal themselves
// input is assumed to be valid hive format
// function must copy the data if it wishes to retain it
type Unmarshaler interface {
	UnmarshalHive(data []byte, depth byte) error
}

var unmarshalerType = reflect.TypeOf((*Unmarshaler)(nil)).Elem()

// A UnmarshalerError represents an error from calling a UnarshalHive method
type UnmarshalerError struct {
	Type reflect.Type
	Err  error
}

func (e UnmarshalerError) Error() string {
	return fmt.Sprintf("error calling UnmarshalHive for type %s: %v", e.Type, e.Err)
}

// UnmarshalTypeError is returned when we're unable to decode data into a given type
type UnmarshalTypeError struct {
	Value []byte
	Type  reflect.Type
}

func (e UnmarshalTypeError) Error() string {
	return fmt.Sprintf("cannot unmarshal %q into Go value of type %s", e.Value, e.Type.String())
}

// An InvalidUnmarshalError describes an invalid argument passed to Unmarshal.
// (The argument to Unmarshal must be a non-nil pointer.)
type InvalidUnmarshalError struct {
	Type reflect.Type
}

func (e InvalidUnmarshalError) Error() string {
	if e.Type == nil {
		return "unmarshal(nil)"
	}

	if e.Type.Kind() != reflect.Ptr {
		return fmt.Sprintf("unmarshal(non-pointer %s)", e.Type)
	}
	return fmt.Sprintf("unmarshal(nil %s)", e.Type)
}

// decode state holds information shared while decoding
type decodeState struct {
	depth byte
}

func (d *decodeState) error(err error) {
	panic(hiveError{err})
}

func (d *decodeState) unmarshalError(data []byte, v reflect.Value) {
	d.error(UnmarshalTypeError{data, v.Type()})
}

type decoderFunc func(*decodeState, []byte, reflect.Value)

var decoderCache sync.Map // map[reflect.Type]decoderFunc

func typeDecoder(t reflect.Type) decoderFunc {
	if fi, ok := decoderCache.Load(t); ok {
		return fi.(decoderFunc)
	}

	// To deal with recursive types, populate the map with an
	// indirect func before we build it. This type waits on the
	// real func (f) to be ready and then calls it. This indirect
	// func is only used for recursive types.
	var (
		wg sync.WaitGroup
		f  decoderFunc
	)
	wg.Add(1)
	defer wg.Done()
	fi, loaded := decoderCache.LoadOrStore(t, decoderFunc(func(d *decodeState, data []byte, v reflect.Value) {
		wg.Wait()
		f(d, data, v)
	}))
	if loaded {
		return fi.(decoderFunc)
	}

	// Compute the real decoder and replace the indirect func with it.
	f = newTypeDecoder(t)
	decoderCache.Store(t, f)
	return f
}

// newTypeDecoder constructs an decoderFunc for a type.
func newTypeDecoder(t reflect.Type) decoderFunc {
	// need to check whether a *type implements unmarshaler type, because it was
	// checked for a pointer in the top unmarshal function, and stripped of it
	if reflect.PtrTo(t).Implements(unmarshalerType) {
		return unmarshalerDecoder
	}

	switch t.Kind() {
	case reflect.Bool:
		return boolDecoder
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return intDecoder
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return uintDecoder
	case reflect.Float32, reflect.Float64:
		return floatDecoder
	case reflect.String:
		return stringDecoder
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			// []byte
			return byteSliceDecoder
		}
		return newSliceDecoder(t)
	case reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			// [x]byte
			return byteArrayDecoder
		}
		return newArrayDecoder(t)
	case reflect.Map:
		return newMapDecoder(t)
	case reflect.Struct:
		return newStructDecoder(t)
	case reflect.Interface:
		return interfaceDecoder
	case reflect.Ptr:
		return newPtrDecoder(t)
	default:
		return unsupportedTypeDecoder
	}
	return nil
}

func unmarshalerDecoder(d *decodeState, data []byte, v reflect.Value) {
	// need to take addr here because it was stripped down in the top unmarshal function
	um := v.Addr().Interface().(Unmarshaler)
	if err := um.UnmarshalHive(data, d.depth); err != nil {
		d.error(UnmarshalerError{v.Type(), err})
	}
}

func unsupportedTypeDecoder(d *decodeState, _ []byte, v reflect.Value) {
	d.error(UnsupportedTypeError{Type: v.Type()})
}

func isNil(data []byte) bool {
	switch {
	case len(data) == 0:
		return true
	case len(data) == 2:
		return reflect.DeepEqual(data, Nil)
	default:
		return false
	}
}

func boolDecoder(d *decodeState, data []byte, v reflect.Value) {
	switch string(data) {
	case "true":
		v.SetBool(true)
	case "false":
		v.SetBool(false)
	default:
		d.unmarshalError(data, v)
	}
}

func intDecoder(d *decodeState, data []byte, v reflect.Value) {
	n, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil || v.OverflowInt(n) {
		d.unmarshalError(data, v)
	}
	v.SetInt(n)
}

func uintDecoder(d *decodeState, data []byte, v reflect.Value) {
	n, err := strconv.ParseUint(string(data), 10, 64)
	if err != nil || v.OverflowUint(n) {
		d.unmarshalError(data, v)
	}
	v.SetUint(n)
}

func floatDecoder(d *decodeState, data []byte, v reflect.Value) {
	n, err := strconv.ParseFloat(string(data), v.Type().Bits())
	if err != nil || v.OverflowFloat(n) {
		d.unmarshalError(data, v)
	}
	v.SetFloat(n)
}

func stringDecoder(d *decodeState, data []byte, v reflect.Value) {
	v.SetString(string(data))
}

type sliceDecoder struct {
	elementDecoder decoderFunc
}

func (sd sliceDecoder) decode(d *decodeState, data []byte, v reflect.Value) {
	if isNil(data) {
		v.Set(reflect.MakeSlice(v.Type(), 0, 0))
		return
	}

	slicer := newSlicer(data, d.depth+2)
	n := slicer.numSlices()
	v.Set(reflect.MakeSlice(v.Type(), n, n))

	d.depth = d.depth + 1
	for i := 0; i < slicer.numSlices(); i++ {
		sd.elementDecoder(d, slicer.slice(i, 1), v.Index(i))
	}
	d.depth = d.depth - 1
}

func newSliceDecoder(t reflect.Type) decoderFunc {
	dec := sliceDecoder{typeDecoder(t.Elem())}
	return dec.decode
}

func byteSliceDecoder(d *decodeState, data []byte, v reflect.Value) {
	b := append([]byte(nil), data...) // copy data
	v.Set(reflect.ValueOf(b))
}

type arrayDecoder struct {
	elementDecoder decoderFunc
}

func (ad arrayDecoder) decode(d *decodeState, data []byte, v reflect.Value) {
	v.Set(reflect.Zero(v.Type()))
	if isNil(data) {
		return
	}

	slicer := newSlicer(data, d.depth+2)
	n := slicer.numSlices()

	if v.Len() != n {
		d.error(fmt.Errorf("decoding array of len %d, got %d elements", v.Len(), n))
	}

	d.depth = d.depth + 1
	for i := 0; i < slicer.numSlices(); i++ {
		ad.elementDecoder(d, slicer.slice(i, 1), v.Index(i))
	}
	d.depth = d.depth - 1
}

func newArrayDecoder(t reflect.Type) decoderFunc {
	dec := arrayDecoder{typeDecoder(t.Elem())}
	return dec.decode
}

func byteArrayDecoder(d *decodeState, data []byte, v reflect.Value) {
	if len(data) != v.Len() {
		d.error(fmt.Errorf("decoding byte array of len %d, got %d elements", v.Len(), len(data)))
	}
	b := append([]byte(nil), data...) // copy data
	reflect.Copy(v, reflect.ValueOf(b))
}

type mapDecoder struct {
	keyDecoder   decoderFunc
	valueDecoder decoderFunc
}

func (md mapDecoder) decode(d *decodeState, data []byte, v reflect.Value) {
	if isNil(data) {
		v.Set(reflect.MakeMapWithSize(v.Type(), 0))
		return
	}

	// same as sequence, but fields are mappings delimited by d.depth + 3
	slicer := newSlicer(data, d.depth+2)

	v.Set(reflect.MakeMapWithSize(v.Type(), slicer.numSlices()))

	// if we're decoding map[int]string
	// then keyValue is new(int) == *int
	//      valValue is new(string) == *string
	// decoder decodes recursively and then we add values to the map
	// map[*keyValue] = *valValue
	// these are reused for all key-value pairs
	keyValue := reflect.New(v.Type().Key())
	valValue := reflect.New(v.Type().Elem())

	mapDelim := d.depth + 3

	d.depth = d.depth + 2
	for i := 0; i < slicer.numSlices(); i++ {
		iterSlicer := newSlicer(slicer.slice(i, 1), mapDelim)
		if iterSlicer.numSlices() != 2 {
			d.unmarshalError(data, v)
		}
		md.keyDecoder(d, iterSlicer.slice(0, 1), keyValue.Elem())
		md.valueDecoder(d, iterSlicer.slice(1, 1), valValue.Elem())
		v.SetMapIndex(keyValue.Elem(), valValue.Elem())
	}
	d.depth = d.depth - 2
}

func newMapDecoder(t reflect.Type) decoderFunc {
	dec := mapDecoder{typeDecoder(t.Key()), typeDecoder(t.Elem())}
	return dec.decode
}

func interfaceDecoder(d *decodeState, data []byte, v reflect.Value) {
	if isNil(data) {
		return
	}
	elem := v.Elem()
	typeDecoder(elem.Type())(d, data, elem)
}

type ptrDecoder struct {
	elemDecoder decoderFunc
}

func (pe ptrDecoder) decode(d *decodeState, data []byte, v reflect.Value) {
	if isNil(data) {
		return // leave it nil
	}
	v.Set(reflect.New(v.Type().Elem()))
	pe.elemDecoder(d, data, v.Elem())
}

func newPtrDecoder(t reflect.Type) decoderFunc {
	dec := ptrDecoder{typeDecoder(t.Elem())}
	return dec.decode
}

type structDecoder struct {
	complexity int
	fields     []field
}

func (sd structDecoder) decode(d *decodeState, data []byte, v reflect.Value) {
	typ := v.Type()
	v.Set(reflect.Zero(typ))

	slicer := newSlicer(data, d.depth+1)
	if slicer.numSlices() == 0 {
		return // empty struct
	}
	if slicer.numSlices() != sd.complexity+1 {
		// not enough data
		d.unmarshalError(data, v)
	}

	offset := 0
	for i := range sd.fields {
		f := &sd.fields[i]
		fv, found := f.findNested(v)
		if !found {
			d.error(fmt.Errorf("can't find %q field", f.name))
		}
		length := f.complexity + 1
		f.decoder(d, slicer.slice(offset, length), fv)
		offset += length
	}

	if offset != slicer.numSlices() {
		d.error(fmt.Errorf("leftover data: %v", slicer.slice(offset, slicer.numSlices()-offset)))
	}
}

func newStructDecoder(t reflect.Type) decoderFunc {
	dec := structDecoder{
		fields:     cachedTypeFields(t),
		complexity: cachedComplexity(t),
	}
	return dec.decode
}
