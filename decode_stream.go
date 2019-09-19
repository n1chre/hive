package hive

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"reflect"
)

// Decoder knows how to decode some value
type Decoder interface {
	// Decode decodes data into the given interface
	// returns io.EOF on end of stream
	Decode(interface{}) error
}

// decoder is used for decoding data
// It can decode a single value, or can decode the whole stream until EOF
// One record is decoded from one line of data. Default line delimiter is \n, but can be changed
type decoder struct {
	*bufio.Scanner
}

// NewDecoder creates a new Decoder to decode the input reader with '\n' as line delimiter
func NewDecoder(r io.Reader) Decoder {
	return NewDecoderWithLineDelimiter(r, '\n')
}

// NewDecoderWithLineDelimiter creates a new Decoder to decode the input reader with a given line delimiter
func NewDecoderWithLineDelimiter(r io.Reader, lineDelimiter byte) Decoder {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 100*1024), 10*1024*1024)
	scanner.Split(splitBy(lineDelimiter))
	return &decoder{scanner}
}

// Decode decodes the current line into the given interface
// interface should be a pointer (addressable)
// returns io.EOF when there's no more lines
func (dec *decoder) Decode(v interface{}) error {
	if !dec.Scanner.Scan() {
		if err := dec.Scanner.Err(); err != nil {
			return err
		}
		return io.EOF
	}

	return Unmarshal(dec.Scanner.Bytes(), v)
}

// DecodeAll will decode all values from the stream (until Decode doesn't return io.EOF)
// All values in the channel are going to be of the given type
// Because this function is blocking, channel needs to be created before calling this function and can be closed after it returns
// Returns error if decoding fails or if context is done
func DecodeAll(ctx context.Context, dec Decoder, typ reflect.Type, ch chan<- interface{}) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			v := reflect.New(typ)
			if err := dec.Decode(v.Interface()); err != nil {
				if err == io.EOF {
					return nil
				}
				return fmt.Errorf("decode error: %v", err)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case ch <- reflect.Indirect(v).Interface():
				break
			}
		}
	}
}

func splitBy(delimiter byte) bufio.SplitFunc {
	// copied from bufio implementation of bufio.SplitLines, the only difference is that it splits by any delimiter
	// https://golang.org/src/bufio/scan.go?#L345
	return func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}
		if i := bytes.IndexByte(data, delimiter); i >= 0 {
			// We have a full newline-terminated line.
			return i + 1, data[:i], nil
		}
		// If we're at EOF, we have a final, non-terminated line. Return it.
		if atEOF {
			return len(data), data, nil
		}
		// Request more data.
		return 0, nil, nil
	}
}
