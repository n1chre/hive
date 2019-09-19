package hive

import (
	"fmt"
	"reflect"
	"testing"
)

type testCaseDecode struct {
	in  string
	out interface{}
}

func testDecoderHelper(t *testing.T, cases []testCaseDecode, getDecoder func(reflect.Type) decoderFunc) {
	for i, c := range cases {
		t.Run(fmt.Sprintf("case-%d", i+1), func(t *testing.T) {
			typ := reflect.TypeOf(c.out)
			ptr := reflect.New(typ)
			var d decodeState
			getDecoder(typ)(&d, []byte(c.in), ptr.Elem())
			have := ptr.Elem().Interface()
			if !reflect.DeepEqual(have, c.out) {
				t.Errorf("in: %q\n\thave:\t%v\n\texpect:\t%v", c.in, have, c.out)
			}
		})
	}
}

func testDecoder(t *testing.T, dec decoderFunc, cases []testCaseDecode) {
	testDecoderHelper(t, cases, func(_ reflect.Type) decoderFunc { return dec })
}

func testDecoderAny(t *testing.T, cases []testCaseDecode) {
	testDecoderHelper(t, cases, typeDecoder)
}

func TestBoolDecoder(t *testing.T) {
	testDecoder(t, boolDecoder, []testCaseDecode{
		{
			in:  "true",
			out: true,
		},
		{
			in:  "false",
			out: false,
		},
	})
}

func TestIntDecoder(t *testing.T) {
	testDecoder(t, intDecoder, []testCaseDecode{
		{
			in:  "-1",
			out: int(-1),
		},
		{
			in:  "-1",
			out: int8(-1),
		},
		{
			in:  "-1",
			out: int16(-1),
		},
		{
			in:  "-1",
			out: int32(-1),
		},
		{
			in:  "-1",
			out: int64(-1),
		},
	})
}

func TestUintDecoder(t *testing.T) {
	testDecoder(t, uintDecoder, []testCaseDecode{
		{
			in:  "1",
			out: uint(1),
		},
		{
			in:  "1",
			out: uint8(1),
		},
		{
			in:  "1",
			out: uint16(1),
		},
		{
			in:  "1",
			out: uint32(1),
		},
		{
			in:  "1",
			out: uint64(1),
		},
		{
			in:  "1",
			out: uintptr(1),
		},
	})
}

func TestFloatDecoder(t *testing.T) {
	testDecoder(t, floatDecoder, []testCaseDecode{
		{
			in:  "3.2",
			out: float32(3.2),
		},
		{
			in:  "6.4",
			out: float64(6.4),
		},
	})
}

func TestStringDecoder(t *testing.T) {
	testDecoder(t, stringDecoder, []testCaseDecode{
		{
			in:  "",
			out: "",
		},
		{
			in:  "foo",
			out: "foo",
		},
	})
}

func TestSequenceDecoder(t *testing.T) {
	intSliceDecoder := newSliceDecoder(reflect.TypeOf([]int{}))
	testDecoder(t, intSliceDecoder, []testCaseDecode{
		{
			in:  "",
			out: []int{},
		},
		{
			in:  "1",
			out: []int{1},
		},
		{
			in:  "1\x022\x023",
			out: []int{1, 2, 3},
		},
	})

	stringSliceDecoder := newSliceDecoder(reflect.TypeOf([]string{}))
	testDecoder(t, stringSliceDecoder, []testCaseDecode{
		{
			in:  "",
			out: []string{},
		},
		{
			in:  "foo",
			out: []string{"foo"},
		},
		{
			in:  "foo\x02bar\x02baz",
			out: []string{"foo", "bar", "baz"},
		},
	})

	intArray3Decoder := newArrayDecoder(reflect.TypeOf([3]int{}))
	testDecoder(t, intArray3Decoder, []testCaseDecode{
		{
			in:  "2\x024\x028",
			out: [3]int{2, 4, 8},
		},
	})
}

func TesetMapDecoder(t *testing.T) {
	int2strMapDecoder := newMapDecoder(reflect.TypeOf(map[int]string{}))
	testDecoder(t, int2strMapDecoder, []testCaseDecode{
		{
			in:  "",
			out: map[int]string{},
		},
		{
			in:  "1\x03one",
			out: map[int]string{1: "one"},
		},
		{
			in:  "1\x03one\x022\x03two",
			out: map[int]string{1: "one", 2: "two"},
		},
	})

	str2intMapDecoder := newMapDecoder(reflect.TypeOf(map[string]int{}))
	testDecoder(t, str2intMapDecoder, []testCaseDecode{
		{
			in:  "",
			out: map[string]int{},
		},
		{
			in:  "one\x031",
			out: map[string]int{"one": 1},
		},
		{
			in:  "one\x031\x02two\x032",
			out: map[string]int{"one": 1, "two": 2},
		},
	})
}

func TestPtrDecoder(t *testing.T) {
	x := 2
	intPtrDecoder := newPtrDecoder(reflect.TypeOf(&x))
	testDecoder(t, intPtrDecoder, []testCaseDecode{
		{
			in:  "2",
			out: &x,
		},
	})
}

func TestStructDecoder(t *testing.T) {
	type foo struct {
		I  int
		S  string
		SS []int
		M  map[string]int
	}

	testDecoderAny(t, []testCaseDecode{
		{
			in: "0\x01\x01\\N\x01\\N",
			out: foo{
				// internals get initialized, same as json
				SS: []int{},
				M:  map[string]int{},
			},
		},
		{
			in: "1\x01str\x012\x023\x01four\x034",
			out: foo{
				I:  1,
				S:  "str",
				SS: []int{2, 3},
				M:  map[string]int{"four": 4},
			},
		},
	})
}

func TestComplexDecode(t *testing.T) {
	type foo struct {
		Matrix [][]int
		Nested struct {
			Map map[string][]int
		}
	}

	testDecoderAny(t, []testCaseDecode{
		{
			in: "\\N\x01\\N",
			out: foo{
				Matrix: [][]int{},
				Nested: struct {
					Map map[string][]int
				}{
					Map: map[string][]int{},
				},
			},
		},
		{
			in: "1\x032\x023\x034\x01five\x035\x045",
			out: foo{
				Matrix: [][]int{{1, 2}, {3, 4}},
				Nested: struct{ Map map[string][]int }{
					Map: map[string][]int{"five": {5, 5}},
				},
			},
		},
	})
}

func TestDecodeNested(t *testing.T) {
	type foo struct {
		S struct {
			S1 []struct {
				M map[int]int
			}
		}
	}

	testDecoderAny(t, []testCaseDecode{
		{
			in: "",
			out: foo{
				S: struct {
					S1 []struct {
						M map[int]int
					}
				}{},
			},
		},
		{
			in: "1\x042",
			out: foo{
				S: struct {
					S1 []struct {
						M map[int]int
					}
				}{
					S1: []struct {
						M map[int]int
					}{
						{M: map[int]int{1: 2}},
					},
				},
			},
		},
	})
}

func TestEmbededStructs(t *testing.T) {
	type foo struct {
		A int
		B int
	}

	type bar struct {
		F1 foo
		F2 *foo
		S1 struct {
			C int
			D int
		}
		S2 *struct {
			C int
			D int
		}
	}

	testDecoderAny(t, []testCaseDecode{
		{
			in:  "",
			out: bar{},
		},
		{
			in: "1\x012\x013\x014\x015\x016\x017\x018",
			out: bar{
				F1: foo{1, 2},
				F2: &foo{3, 4},
				S1: struct {
					C int
					D int
				}{5, 6},
				S2: &struct {
					C int
					D int
				}{7, 8},
			},
		},
	})
}

func TestAnonymousStruct(t *testing.T) {
	type Foo struct {
		X int
	}

	type Baz int

	type bar struct {
		Foo
		Baz
		S1 struct {
			Foo
		}
		S2 struct {
			Foo
		}
	}

	testDecoderAny(t, []testCaseDecode{
		{
			in:  "",
			out: bar{},
		},
		{
			in: "1\x012\x013\x014",
			out: bar{
				Foo: Foo{X: 1},
				Baz: Baz(2),
				S1: struct {
					Foo
				}{Foo: Foo{X: 3}},
				S2: struct {
					Foo
				}{Foo: Foo{X: 4}},
			},
		},
	})
}

func TestUnmarshaler(t *testing.T) {
	um := testStructUnmarshaler{}
	want := testStructUnmarshaler{1, 2}
	if err := Unmarshal([]byte("foo"), &um); err != nil {
		t.Fatalf("error while unmarshal: %v", err)
	} else if !reflect.DeepEqual(um, want) {
		t.Fatalf("wrong unmarshal result:\n\thave: %+v\n\twant: %+v", um, want)
	}
}

type testStructUnmarshaler struct {
	I int
	J int
}

func (um *testStructUnmarshaler) UnmarshalHive([]byte, byte) error {
	um.I = 1
	um.J = 2
	return nil
}
