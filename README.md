# hive
Implementation of the LazySimpleSerDe for Hive in Go

## Format

When using this with hive, you can create your table using this and all the files will be in the LazySimpleSerDe format.

```sql
CREATE EXTERNAL TABLE t (
    -- columns
)
-- options
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
COLLECTION ITEMS TERMINATED BY '\002'
MAP KEYS TERMINATED BY '\003'
STORED AS TEXTFILE
-- options
LOCATION 'hdfs://some-path'
```

This format is pretty simple. It uses `\x01`, `\x02`, ... as delimiters, in the following way:

Delimiter starts as `1` (`\x01`).

Simple types (`bool`, `int`, `float`, `string`, ...) are encoded and decoded as is.

Structs are formatted as:

- format each of the fields recursively without chaning the delimiter value
- join them with the `delimiter`

Slices are formatted as:

- format each of the elements recursively with `delimiter+1`
- join them with `delimiter+2`

Maps are formatted as:

- format each of the key, values recursively with `delimiter+2`
- join key and value with `delimiter+3` and then join all key value pairs with `delimiter+2`

### Example

```golang
type Foo struct {
    B  bool
    I  int
    S  string
    SS []int
    M  map[int]int
}

foo = Foo{true, 1, "two", []int{3,4,5}, map[int]int{6:7, 8:9}}
// decoded from/encoded as:
"true\x011\x01two\x013\x024\x025\x016\x037\x028\x039"
// map ordering is ambiguous so 8:9 might get encoded before 6:7
```

## Code examples

### Marshal and Unmarshal

```golang
package main

type Foo struct {
    B  bool
    I  int
    S  string
    SS []int
    M  map[int]int
}

func main() {
    foo := Foo{true, 1, "two", []int{3,4,5}, map[int]int{6:7, 8:9}}
    data, err := Marshal(foo)
    if err != nil {
        panic(err)
    }

    var foo2 Foo
    if err = Unmarshal(data, &foo2); err != nil {
        panic(err)
    }
}
```

### Encoder and Decoder

```golang
package main

import (
    "io"
    "os"
)

type Foo struct {
    B  bool
    I  int
    S  string
    SS []int
    M  map[int]int
}

func main() {
    dec := NewDecoder(os.Stdin)
    enc := NewEncoder(os.Stdout)
    var foo Foo

    for {
        if err := dec.Decode(&foo); err != nil {
            if err == io.EOF {
                break
            }
            panic(err)
        }
        // do something with foo
        if err := enc.Encode(foo); err != nil {
            panic(err)
        }
    }
}
```

### Encoder and Decoder stream

```golang
package main

import (
    "context"
    "os"
    "reflect"

    "golang.org/x/sync/errgroup"
)

type Foo struct {
    B  bool
    I  int
    S  string
    SS []int
    M  map[int]int
}

func main() {
    dec := NewDecoder(os.Stdin)
    enc := NewEncoder(os.Stdout)
    inch := make(chan interface{})
    outch := make(chan interface{})

    g, ctx := errgroup.WithContext(context.TODO())
    g.Go(func() error {
        defer close(inch)
        return DecodeAll(ctx, dec, reflect.TypeOf(Foo{}), inch)
    })
    g.Go(func() error {
        defer close(outch)
        for foo := range inch {
            // do something with Foo
            outch <- foo
        }
        return nil
    })
    g.Go(func() error {
        return EncodeAll(ctx, enc, outch)
    })

    if err := g.Wait(); err != nil {
        panic(err)
    }
}
```
