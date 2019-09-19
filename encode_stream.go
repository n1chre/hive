package hive

import (
	"context"
	"fmt"
	"io"
)

// Encoder knows how to encode some value
type Encoder interface {
	// Encode encodes data from the given interface
	Encode(interface{}) error
}

// encoder is used for encoding data
// It can encode a single value, or can decode the whole channel of values of any type
// After each record is encoded, line delimiter is written to the underlying writer
// Default line delimiter is \n, but can be changed
type encoder struct {
	writer        io.Writer
	lineDelimiter byte
}

// NewEncoder creates a new Encoder to encode values with '\n' as line delimiter
func NewEncoder(w io.Writer) Encoder {
	return NewEncoderWithLineDelimiter(w, '\n')
}

// NewEncoderWithLineDelimiter creates a new Encoder to encode values with a given line delimiter
func NewEncoderWithLineDelimiter(w io.Writer, lineDelimiter byte) Encoder {
	return &encoder{writer: w, lineDelimiter: lineDelimiter}
}

// Encode encodes the given value and writes it to the underlying writer
func (enc *encoder) Encode(v interface{}) error {
	e := newEncodeState()
	defer e.release()

	var err error
	if err = e.marshal(v); err == nil {
		e.WriteByte(enc.lineDelimiter)
		_, err = enc.writer.Write(e.Bytes())
	}

	return err
}

// EncodeAll will encode all values from the given channel
// Because this function is blocking, channel needs to be created and closed outside of this function
// Returns error if encoding fails or if context is done
func EncodeAll(ctx context.Context, enc Encoder, ch <-chan interface{}) error {
	for {
		select {
		case v, more := <-ch:
			if !more {
				return nil
			}
			if err := enc.Encode(v); err != nil {
				return fmt.Errorf("encode error: %v", err)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
