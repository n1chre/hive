package hive

import (
	"fmt"
	"reflect"
	"testing"
)

func TestSlicer(t *testing.T) {
	for i, c := range []struct {
		in     string
		delim  string
		slices []string
	}{
		{
			in:     "",
			delim:  "any",
			slices: []string{},
		},
		{
			in:     "aa-bb",
			delim:  "-",
			slices: []string{"aa", "bb"},
		},
		{
			in:     "aa-bb",
			delim:  "a",
			slices: []string{"", "", "-bb"},
		},
		{
			in:     "x\x01y\x02y\x02y\x01x",
			delim:  "\x01",
			slices: []string{"x", "y\x02y\x02y", "x"},
		},
	} {
		t.Run(fmt.Sprintf("case-%d", i+1), func(t *testing.T) {
			s := newSlicer([]byte(c.in), c.delim[0])
			if s.numSlices() != len(c.slices) {
				t.Fatalf("wrong number of slices\n\thave: %d\n\twant: %d", s.numSlices(), len(c.slices))
			}
			for i, sl := range c.slices {
				if have := s.slice(i, 1); !reflect.DeepEqual(have, []byte(sl)) {
					t.Fatalf("slice %d wrong\n\thave: %q\n\twant: %q", have, sl)
				}
			}
		})
	}
}

func TestComplexity(t *testing.T) {
	for i, c := range []struct {
		iface interface{}
		want  int
	}{
		{
			iface: "ss",
			want:  0,
		},
		{
			iface: struct {
				I int
				J int
			}{},
			want: 1,
		},
		{
			iface: struct {
				I int
				J struct {
					K int
					L int
				}
			}{1, struct {
				K int
				L int
			}{2, 3}},
			want: 2,
		},
		{
			iface: struct {
				I int
				J struct{}
			}{1, struct{}{}},
			want: 0,
		},
		{
			iface: struct { // 0
				S struct { // 0
					S1 []struct { // 0
						M map[int]int // 0
					}
				}
			}{
				S: struct {
					S1 []struct {
						M map[int]int
					}
				}{
					S1: []struct {
						M map[int]int
					}{},
				},
			},
			want: 0,
		},
	} {
		t.Run(fmt.Sprintf("case-%d", i+1), func(t *testing.T) {
			typ := reflect.TypeOf(c.iface)
			have := complexity(typ)
			if have != c.want {
				t.Fatalf("wrong complexity for %s\n\thave: %d\n\twant: %d", typ.String(), have, c.want)
			}
		})
	}
}

func TestMarshal2Unmarshal(t *testing.T) {
	type foo struct {
		I  int
		S  string
		F  *float64
		St struct {
			F *float64
		}
	}
	f := float64(4.2)
	is := []interface{}{
		nil,
		struct{}{},
		1,
		"foo",
		[]byte("bar"),
		Nil,
		[]int{2, 3, 4},
		map[int][]map[int]int{1: {{2: 3}}},
		&f,
		foo{
			I:  4,
			S:  "four",
			F:  &f,
			St: struct{ F *float64 }{&f},
		},
	}

	for i, interf := range is {
		t.Run(fmt.Sprintf("case-%d", i+1), func(t *testing.T) {
			data, err := Marshal(interf)
			if err != nil {
				t.Errorf("unable to marshal %v: %v", interf, err)
			}
			if interf == nil {
				return
			}

			typ := reflect.TypeOf(interf)
			ptr := reflect.New(typ)

			if err = Unmarshal(data, ptr.Interface()); err != nil {
				t.Errorf("unable to unmarshal %q: %v", data, err)
			}

			if !reflect.DeepEqual(interf, ptr.Elem().Interface()) {
				t.Errorf("unmarshal(marshal(obj)) != obj\n\tobj:\t%v", interf)
			}
		})
	}
}
