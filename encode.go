package hive

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"sync"
)

// Marshal will return Hive encoding of the given interface
// returns an error if can't be encoded
func Marshal(v interface{}) ([]byte, error) {
	e := newEncodeState()
	defer e.release()

	if err := e.marshal(v); err != nil {
		return nil, err
	}

	return append([]byte(nil), e.Bytes()...), nil
}

// Marshaler is the interface implemented by types that can marshal themselves into valid Hive format.
type Marshaler interface {
	MarshalHive(depth byte) ([]byte, error)
}

var marshalerType = reflect.TypeOf((*Marshaler)(nil)).Elem()

// A MarshalerError represents an error from calling a MarshalHive method
type MarshalerError struct {
	Type reflect.Type
	Err  error
}

func (e MarshalerError) Error() string {
	return fmt.Sprintf("error calling MarshalHive for type %s: %v", e.Type, e.Err)
}

// UnsupportedValueError is returned when value can't be encoded
type UnsupportedValueError struct {
	Value reflect.Value
	Str   string
}

func (e UnsupportedValueError) Error() string {
	return "unsupported value: " + e.Str
}

type encodeState struct {
	bytes.Buffer
	scratch [64]byte
	depth   byte
}

var encodeStatePool sync.Pool

func newEncodeState() *encodeState {
	if v := encodeStatePool.Get(); v != nil {
		e := v.(*encodeState)
		e.Reset()
		return e
	}
	return new(encodeState)
}

func (e *encodeState) release() {
	encodeStatePool.Put(e)
}

func (e *encodeState) error(err error) {
	panic(hiveError{err})
}

func (e *encodeState) marshal(v interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if he, ok := r.(hiveError); ok {
				err = he.error
			} else {
				panic(r)
			}
		}
	}()
	e.reflectValue(reflect.ValueOf(v))
	return nil
}

func (e *encodeState) writeNil() {
	e.Write(Nil)
}

func (e *encodeState) reflectValue(v reflect.Value) {
	valueEncoder(v)(e, v)
}

type encoderFunc func(e *encodeState, v reflect.Value)

func invalidValueEncoder(e *encodeState, v reflect.Value) {
	e.writeNil()
}

func valueEncoder(v reflect.Value) encoderFunc {
	if !v.IsValid() {
		return invalidValueEncoder
	}
	return typeEncoder(v.Type())
}

var encoderCache sync.Map // map[reflect.Type]encoderFunc

func typeEncoder(t reflect.Type) encoderFunc {
	if fi, ok := encoderCache.Load(t); ok {
		return fi.(encoderFunc)
	}

	// To deal with recursive types, populate the map with an
	// indirect func before we build it. This type waits on the
	// real func (f) to be ready and then calls it. This indirect
	// func is only used for recursive types.
	var (
		wg sync.WaitGroup
		f  encoderFunc
	)
	wg.Add(1)
	defer wg.Done()
	fi, loaded := encoderCache.LoadOrStore(t, encoderFunc(func(e *encodeState, v reflect.Value) {
		wg.Wait()
		f(e, v)
	}))
	if loaded {
		return fi.(encoderFunc)
	}

	// Compute the real encoder and replace the indirect func with it.
	f = newTypeEncoder(t)
	encoderCache.Store(t, f)
	return f
}

func unsupportedTypeEncoder(e *encodeState, v reflect.Value) {
	e.error(UnsupportedTypeError{Type: v.Type()})
}

// newTypeEncoder constructs an encoderFunc for a type.
func newTypeEncoder(t reflect.Type) encoderFunc {
	if t.Implements(marshalerType) {
		return marshalerEncoder
	}

	if reflect.PtrTo(t).Implements(marshalerType) {
		return marshalerPtrEncoder
	}

	switch t.Kind() {
	case reflect.Bool:
		return boolEncoder
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return intEncoder
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return uintEncoder
	case reflect.Float32:
		return float32Encoder
	case reflect.Float64:
		return float64Encoder
	case reflect.String:
		return stringEncoder
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return byteSliceEncoder
		}
		return newSequenceEncoder(t, true)
	case reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			return byteArrayEncoder
		}
		return newSequenceEncoder(t, false)
	case reflect.Map:
		return newMapEncoder(t)
	case reflect.Struct:
		return newStructEncoder(t)
	case reflect.Interface:
		return interfaceEncoder
	case reflect.Ptr:
		return newPtrEncoder(t)
	default:
		return unsupportedTypeEncoder
	}
}

func marshalerEncoder(e *encodeState, v reflect.Value) {
	if v.Kind() == reflect.Ptr && v.IsNil() {
		e.writeNil()
		return
	}

	m := v.Interface().(Marshaler)
	if b, err := m.MarshalHive(e.depth); err == nil {
		e.Write(b)
	} else {
		e.error(MarshalerError{v.Type(), err})
	}
}

func marshalerPtrEncoder(e *encodeState, v reflect.Value) {
	// *v implements marshaler
	vp := reflect.New(v.Type())
	vp.Elem().Set(v)

	m := vp.Interface().(Marshaler)
	if b, err := m.MarshalHive(e.depth); err == nil {
		e.Write(b)
	} else {
		e.error(MarshalerError{v.Type(), err})
	}
}

func boolEncoder(e *encodeState, v reflect.Value) {
	if v.Bool() {
		e.WriteString("true")
	} else {
		e.WriteString("false")
	}
}

func intEncoder(e *encodeState, v reflect.Value) {
	b := strconv.AppendInt(e.scratch[:0], v.Int(), 10)
	e.Write(b)
}

func uintEncoder(e *encodeState, v reflect.Value) {
	b := strconv.AppendUint(e.scratch[:0], v.Uint(), 10)
	e.Write(b)
}

type floatEncoder int // number of bits

func (bits floatEncoder) encode(e *encodeState, v reflect.Value) {
	// copied from json encoder
	f := v.Float()
	if math.IsInf(f, 0) || math.IsNaN(f) {
		e.error(UnsupportedValueError{v, strconv.FormatFloat(f, 'g', -1, int(bits))})
	}

	// Convert as if by ES6 number to string conversion.
	// Like fmt %g, but the exponent cutoffs are different
	// and exponents themselves are not padded to two digits.
	b := e.scratch[:0]
	abs := math.Abs(f)
	fmt := byte('f')
	// Note: Must use float32 comparisons for underlying float32 value to get precise cutoffs right.
	if abs != 0 {
		if bits == 64 && (abs < 1e-6 || abs >= 1e21) || bits == 32 && (float32(abs) < 1e-6 || float32(abs) >= 1e21) {
			fmt = 'e'
		}
	}
	b = strconv.AppendFloat(b, f, fmt, -1, int(bits))
	if fmt == 'e' {
		// clean up e-09 to e-9
		n := len(b)
		if n >= 4 && b[n-4] == 'e' && b[n-3] == '-' && b[n-2] == '0' {
			b[n-2] = b[n-1]
			b = b[:n-1]
		}
	}

	e.Write(b)
}

var float32Encoder = floatEncoder(32).encode
var float64Encoder = floatEncoder(64).encode

func stringEncoder(e *encodeState, v reflect.Value) {
	if s := v.String(); s != "" {
		e.WriteString(s)
	}
}

type sequenceEncoder struct {
	elementEncoder encoderFunc
	nullable       bool
}

func (se sequenceEncoder) encode(e *encodeState, v reflect.Value) {
	if se.nullable && v.IsNil() {
		e.writeNil()
		return
	}
	delimiter := e.depth + 2
	e.depth = e.depth + 1
	for i, n := 0, v.Len(); i < n; i++ {
		if i > 0 {
			e.WriteByte(delimiter)
		}
		se.elementEncoder(e, v.Index(i))
	}
	e.depth = e.depth - 1
}

func newSequenceEncoder(t reflect.Type, nullable bool) encoderFunc {
	enc := sequenceEncoder{typeEncoder(t.Elem()), nullable}
	return enc.encode
}

var byteSliceType = reflect.TypeOf([]byte{})

func byteSliceEncoder(e *encodeState, v reflect.Value) {
	if v.IsNil() {
		e.writeNil()
		return
	}
	// need to convert, because if we have something like
	// type foo []byte
	// then we can't just convert it to []byte
	e.Write(v.Convert(byteSliceType).Interface().([]byte))
}

func byteArrayEncoder(e *encodeState, v reflect.Value) {
	for i, n := 0, v.Len(); i < n; i++ {
		e.WriteByte(v.Index(i).Interface().(byte))
	}
}

type mapEncoder struct {
	keyEncoder   encoderFunc
	valueEncoder encoderFunc
}

func (me mapEncoder) encode(e *encodeState, v reflect.Value) {
	if v.IsNil() {
		e.writeNil()
		return
	}

	listDelimiter := e.depth + 2
	mapDelimiter := e.depth + 3
	e.depth = e.depth + 2

	isFirst := true
	for _, key := range v.MapKeys() {
		if !isFirst {
			e.WriteByte(listDelimiter)
		}
		isFirst = false
		me.keyEncoder(e, key)
		e.WriteByte(mapDelimiter)
		me.valueEncoder(e, v.MapIndex(key))
	}

	e.depth = e.depth - 2
}

func newMapEncoder(t reflect.Type) encoderFunc {
	enc := mapEncoder{typeEncoder(t.Key()), typeEncoder(t.Elem())}
	return enc.encode
}

func interfaceEncoder(e *encodeState, v reflect.Value) {
	if v.IsNil() {
		e.writeNil()
		return
	}
	e.reflectValue(v.Elem())
}

type ptrEncoder struct {
	elemEncoder encoderFunc
}

func (pe ptrEncoder) encode(e *encodeState, v reflect.Value) {
	if v.IsNil() {
		e.writeNil()
		return
	}
	pe.elemEncoder(e, v.Elem())
}

func newPtrEncoder(t reflect.Type) encoderFunc {
	enc := ptrEncoder{typeEncoder(t.Elem())}
	return enc.encode
}

type structEncoder struct {
	fields []field
}

func (se structEncoder) encode(e *encodeState, v reflect.Value) {
	delimiter := e.depth + 1
	isFirst := true
	for i := range se.fields {
		f := &se.fields[i]
		fv, found := f.findNested(v)
		if !found {
			e.error(fmt.Errorf("can't find %q field", f.name))
		}
		if !isFirst {
			e.WriteByte(delimiter)
		}
		isFirst = false
		f.encoder(e, fv)
	}
}

func newStructEncoder(t reflect.Type) encoderFunc {
	enc := structEncoder{fields: cachedTypeFields(t)}
	return enc.encode
}
