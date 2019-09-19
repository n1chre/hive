package hive

import (
	"fmt"
	"reflect"
	"testing"
)

type testCaseEncode struct {
	in  interface{}
	out string
}

func testEncoderHelper(t *testing.T, cases []testCaseEncode, getEncoder func(reflect.Type) encoderFunc) {
	for i, c := range cases {
		t.Run(fmt.Sprintf("case-%d", i+1), func(t *testing.T) {
			e := newEncodeState()
			val := reflect.ValueOf(c.in)
			getEncoder(val.Type())(e, val)
			have := string(e.Bytes())
			if !reflect.DeepEqual(have, c.out) {
				t.Errorf("in: %v\n\thave:\t%q\n\texpect:\t%q", c.in, have, c.out)
			}
		})
	}
}

func testEncoder(t *testing.T, enc encoderFunc, cases []testCaseEncode) {
	testEncoderHelper(t, cases, func(_ reflect.Type) encoderFunc { return enc })
}

func testEncoderAny(t *testing.T, cases []testCaseEncode) {
	testEncoderHelper(t, cases, typeEncoder)
}

func TestBoolEncoder(t *testing.T) {
	testEncoder(t, boolEncoder, []testCaseEncode{
		{
			in:  true,
			out: "true",
		},
		{
			in:  false,
			out: "false",
		},
	})
}

func TestIntEncoder(t *testing.T) {
	testEncoder(t, intEncoder, []testCaseEncode{
		{
			in:  int(-1),
			out: "-1",
		},
		{
			in:  int8(-1),
			out: "-1",
		},
		{
			in:  int16(-1),
			out: "-1",
		},
		{
			in:  int32(-1),
			out: "-1",
		},
		{
			in:  int64(-1),
			out: "-1",
		},
	})
}

func TestUintEncoder(t *testing.T) {
	testEncoder(t, uintEncoder, []testCaseEncode{
		{
			in:  uint(1),
			out: "1",
		},
		{
			in:  uint8(1),
			out: "1",
		},
		{
			in:  uint16(1),
			out: "1",
		},
		{
			in:  uint32(1),
			out: "1",
		},
		{
			in:  uint64(1),
			out: "1",
		},
		{
			in:  uintptr(1),
			out: "1",
		},
	})
}

func TestFloat32Encoder(t *testing.T) {
	testEncoder(t, float32Encoder, []testCaseEncode{
		{
			in:  float32(3.2),
			out: "3.2",
		},
	})
}

func TestFloat64Encoder(t *testing.T) {
	testEncoder(t, float64Encoder, []testCaseEncode{
		{
			in:  float64(6.4),
			out: "6.4",
		},
	})
}

func TestStringEncoder(t *testing.T) {
	testEncoder(t, stringEncoder, []testCaseEncode{
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

func TestSliceEncoder(t *testing.T) {
	intSliceEncoder := newSequenceEncoder(reflect.TypeOf([]int{}), true)
	testEncoder(t, intSliceEncoder, []testCaseEncode{
		{
			in:  []int{},
			out: "",
		},
		{
			in:  []int{1},
			out: "1",
		},
		{
			in:  []int{1, 2, 3},
			out: "1\x022\x023",
		},
	})

	stringSliceEncoder := newSequenceEncoder(reflect.TypeOf([]string{}), true)
	testEncoder(t, stringSliceEncoder, []testCaseEncode{
		{
			in:  []string{},
			out: "",
		},
		{
			in:  []string{"foo"},
			out: "foo",
		},
		{
			in:  []string{"foo", "bar", "baz"},
			out: "foo\x02bar\x02baz",
		},
	})
}

func TesetMapEncoder(t *testing.T) {
	int2strMapEncoder := newMapEncoder(reflect.TypeOf(map[int]string{}))
	testEncoder(t, int2strMapEncoder, []testCaseEncode{
		{
			in:  map[int]string{},
			out: "",
		},
		{
			in:  map[int]string{1: "one"},
			out: "1\x03one",
		},
		{
			in:  map[int]string{1: "one", 2: "two"},
			out: "1\x03one\x022\x03two",
		},
	})

	str2intMapEncoder := newMapEncoder(reflect.TypeOf(map[string]int{}))
	testEncoder(t, str2intMapEncoder, []testCaseEncode{
		{
			in:  map[string]int{},
			out: "",
		},
		{
			in:  map[string]int{"one": 1},
			out: "one\x031",
		},
		{
			in:  map[string]int{"one": 1, "two": 2},
			out: "one\x031\x02two\x032",
		},
	})
}

func TestPointerEncoder(t *testing.T) {
	x := 1
	testEncoder(t, newPtrEncoder(reflect.TypeOf(&x)), []testCaseEncode{
		{
			in:  &x,
			out: "1",
		},
	})
}

func TestStructEncoder(t *testing.T) {
	type foo struct {
		u  int // unexported and not serialized
		I  int
		S  string
		SS []int
		M  map[string]int
	}

	testEncoderAny(t, []testCaseEncode{
		{
			in:  foo{},
			out: "0\x01\x01\\N\x01\\N",
		},
		{
			in: foo{
				u:  420,
				I:  1,
				S:  "str",
				SS: []int{2, 3},
				M:  map[string]int{"four": 4},
			},
			out: "1\x01str\x012\x023\x01four\x034",
		},
	})
}

func TestComplexEncode(t *testing.T) {
	type foo struct {
		I      int
		Matrix [][]int
		Nested struct {
			Map map[string][]int
		}
	}

	testEncoderAny(t, []testCaseEncode{
		{
			in:  foo{},
			out: "0\x01\\N\x01\\N",
		},
		{
			in: foo{
				I:      7,
				Matrix: [][]int{{1, 2}, {3, 4}},
				Nested: struct{ Map map[string][]int }{
					Map: map[string][]int{"five": {5, 5}},
				},
			},
			out: "7\x011\x032\x023\x034\x01five\x035\x045",
		},
	})
}

func TestMarshal(t *testing.T) {
	f := float64(4.2)
	is := []interface{}{
		1,
		"foo",
		[]byte("bar"),
		Nil,
		[2]byte{'\\', 'N'},
		[]int{2, 3, 4},
		map[int][]map[int]int{1: {{2: 3}}},
		nil,
		&f,
	}

	for i, interf := range is {
		t.Run(fmt.Sprintf("case-%d", i+1), func(t *testing.T) {
			if _, err := Marshal(interf); err != nil {
				t.Errorf("unable to marshal %v: %v", interf, err)
			}
		})
	}
}

func TestMarshaler(t *testing.T) {
	m1 := testStructMarshaler1{}
	if b, err := Marshal(m1); err != nil {
		t.Fatalf("error while marshal of %+v: %v", m1, err)
	} else if !reflect.DeepEqual(string(b), "foo") {
		t.Fatalf("expecting marshal to be 'foo', got %q", b)
	}
	if b, err := Marshal(&m1); err != nil {
		t.Fatalf("error while marshal of %+v: %v", m1, err)
	} else if !reflect.DeepEqual(string(b), "foo") {
		t.Fatalf("expecting marshal to be 'foo', got %q", b)
	}

	m2 := testStructMarshaler2{}
	if b, err := Marshal(&m2); err != nil {
		t.Fatalf("error while marshal of %+v: %v", m1, err)
	} else if !reflect.DeepEqual(string(b), "bar") {
		t.Fatalf("expecting marshal to be 'bar', got %q", b)
	}
}

type testStructMarshaler1 struct {
	I int
	J int
}

func (testStructMarshaler1) MarshalHive(_ byte) ([]byte, error) {
	return []byte("foo"), nil
}

type testStructMarshaler2 struct {
	I int
	J int
}

func (*testStructMarshaler2) MarshalHive(_ byte) ([]byte, error) {
	return []byte("bar"), nil
}
