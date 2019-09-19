package hive

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestStream(t *testing.T) {
	for i, c := range []struct {
		in   []string
		vals []interface{}
	}{
		{
			in:   []string{"1"},
			vals: []interface{}{1},
		},
		{
			in:   []string{"true", "false"},
			vals: []interface{}{true, false},
		},
		{
			in:   []string{"1\x022", "2\x023\x024"},
			vals: []interface{}{[]int{1, 2}, []int{2, 3, 4}},
		},
		// disable this because map iteration can be ambigous, so it could be encoded in 2 different ways
		// {
		// 	in:   []string{"1\x03two\x022\x03one"},
		// 	vals: []interface{}{map[int]string{1: "two", 2: "one"}},
		// },
		{
			in: []string{
				// different types can be decoded from same input
				"1\x01true\x01str",
				"1\x01true\x01str",
				"1\x01true\x01str",
			},
			vals: []interface{}{
				struct {
					I int
					B bool
					S string
				}{I: 1, B: true, S: "str"},
				struct {
					I  int
					SS struct {
						B bool
						S string
					}
				}{
					I: 1,
					SS: struct {
						B bool
						S string
					}{B: true, S: "str"}},
				struct {
					SS struct {
						I int
						B bool
					}
					S string
				}{
					SS: struct {
						I int
						B bool
					}{I: 1, B: true},
					S: "str",
				},
			},
		},
	} {
		t.Run(fmt.Sprintf("case-%d", i+1), func(t *testing.T) {
			if len(c.vals) == 0 {
				t.Fatalf("can't test with no values")
			}

			in := strings.Join(c.in, "\n") + "\n"
			outVals := make([]interface{}, len(c.vals))

			// decode
			dec := NewDecoder(strings.NewReader(in))

			for j, v := range c.vals {
				vv := reflect.New(reflect.TypeOf(v))
				if err := dec.Decode(vv.Interface()); err != nil {
					t.Fatalf("decode error: %v", err)
				}
				outVals[j] = reflect.Indirect(vv).Interface()
			}

			if !reflect.DeepEqual(c.vals, outVals) {
				t.Fatalf("decoded wrong values\n\thave: %v\n\twant: %v", outVals, c.vals)
			}

			// encode
			var output strings.Builder
			enc := NewEncoder(&output)

			for _, v := range outVals {
				if err := enc.Encode(v); err != nil {
					t.Fatalf("encode error: %v", err)
				}
			}

			if have := output.String(); !reflect.DeepEqual(have, in) {
				t.Fatalf("encoded doesn't match\n\thave: %q\n\twant: %q", have, in)
			}
		})
	}
}
