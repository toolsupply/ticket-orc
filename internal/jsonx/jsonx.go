// Package jsonx provides strict JSON decoding for operator configuration.
// It rejects duplicate keys, trailing documents, unknown struct fields, and
// type mismatches without adding a third-party dependency.
package jsonx

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

type frame struct {
	isObject bool
	wantKey  bool
	keys     []string
}

// Validate checks that data contains one complete JSON document and that no
// object contains duplicate keys.
func Validate(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	var stack []frame
	done := false
	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			if !done {
				return fmt.Errorf("unterminated JSON document")
			}
			return nil
		}
		if err != nil {
			return err
		}
		if done {
			return fmt.Errorf("trailing content after JSON document")
		}
		switch value := token.(type) {
		case json.Delim:
			switch value {
			case '{':
				stack = append(stack, frame{isObject: true, wantKey: true})
			case '[':
				stack = append(stack, frame{})
			case '}', ']':
				if len(stack) == 0 {
					return fmt.Errorf("unmatched closing delimiter")
				}
				stack = stack[:len(stack)-1]
				if len(stack) == 0 {
					done = true
				} else if stack[len(stack)-1].isObject {
					stack[len(stack)-1].wantKey = true
				}
			default:
				return fmt.Errorf("unexpected delimiter %q", value)
			}
		case string:
			if len(stack) == 0 {
				done = true
				continue
			}
			current := &stack[len(stack)-1]
			if current.isObject && current.wantKey {
				for _, key := range current.keys {
					if key == value {
						return fmt.Errorf("duplicate key %q", value)
					}
				}
				current.keys = append(current.keys, value)
				current.wantKey = false
			} else if current.isObject {
				current.wantKey = true
			}
		default:
			if len(stack) == 0 {
				done = true
			} else if stack[len(stack)-1].isObject {
				stack[len(stack)-1].wantKey = true
			}
		}
	}
}

// Decode decodes one strict JSON document into out.
func Decode(data []byte, out any) error {
	if err := Validate(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	return decoder.Decode(out)
}
