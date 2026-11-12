package sources

import (
	"bufio"
	"io"
	"unicode/utf8"
)

// Problems an array element can have. A problem is a property of the bytes:
// they will never parse, whoever retries them.
const (
	ProblemTruncated   = "truncated"    // the body ended inside the element
	ProblemTooDeep     = "too_deep"     // nesting beyond Limits.MaxDepth
	ProblemTooLarge    = "too_large"    // larger than Limits.MaxBytes (only a prefix is kept)
	ProblemInvalidUTF8 = "invalid_utf8" // not valid UTF-8 (encoding/json would silently replace it)
	ProblemNotAnArray  = "not_an_array" // the response is not a JSON array at all
	ProblemGarbage     = "garbage"      // bytes that are not an element where one was expected
)

// Element is one top-level element of a JSON array response, as received.
type Element struct {
	Raw     []byte // exact bytes; only the first Limits.MaxBytes when Problem == ProblemTooLarge
	Size    int64  // size of the element as received (>= len(Raw))
	Problem string // "" when the element is complete and within limits
}

// Limits bound what one element may cost. Zero values get defaults.
type Limits struct {
	MaxBytes int // default 1 MiB
	MaxDepth int // default 32
}

func (l Limits) norm() Limits {
	if l.MaxBytes <= 0 {
		l.MaxBytes = 1 << 20
	}
	if l.MaxDepth <= 0 {
		l.MaxDepth = 32
	}
	return l
}

// SplitArray streams a JSON array and returns its elements one by one without
// decoding them. It never recurses and never holds more than Limits.MaxBytes of
// one element, so a malicious or broken body (cut off, nested a million levels
// deep, gigabytes long, not UTF-8) cannot crash the process or exhaust memory;
// the bad element is returned with its Problem and the elements around it are
// still returned. Only an I/O error other than EOF is returned as error.
func SplitArray(r io.Reader, lim Limits) ([]Element, error) {
	lim = lim.norm()
	br := bufio.NewReaderSize(r, 64<<10)
	var out []Element

	b, err := skipSpace(br)
	if err == io.EOF {
		return []Element{{Problem: ProblemNotAnArray}}, nil
	}
	if err != nil {
		return nil, err
	}
	if b != '[' {
		_ = br.UnreadByte()
		rest, err := readCapped(br, lim.MaxBytes)
		if err != nil {
			return nil, err
		}
		return []Element{{Raw: rest.raw, Size: rest.size, Problem: ProblemNotAnArray}}, nil
	}

	first := true
	for {
		b, err := skipSpace(br)
		if err == io.EOF {
			return out, nil // array never closed after a complete element: nothing more was received
		}
		if err != nil {
			return out, err
		}
		if b == ']' {
			return out, nil
		}
		if !first {
			if b != ',' {
				_ = br.UnreadByte()
				rest, err := readCapped(br, lim.MaxBytes)
				if err != nil {
					return out, err
				}
				return append(out, Element{Raw: rest.raw, Size: rest.size, Problem: ProblemGarbage}), nil
			}
			if b, err = skipSpace(br); err == io.EOF {
				return append(out, Element{Problem: ProblemTruncated}), nil
			} else if err != nil {
				return out, err
			}
		}
		first = false
		_ = br.UnreadByte()
		el, err := readElement(br, lim)
		if err != nil && err != io.EOF {
			return out, err
		}
		out = append(out, el)
		if err == io.EOF {
			return out, nil
		}
	}
}

func skipSpace(br *bufio.Reader) (byte, error) {
	for {
		b, err := br.ReadByte()
		if err != nil {
			return 0, err
		}
		switch b {
		case ' ', '\t', '\n', '\r':
			continue
		}
		return b, nil
	}
}

type capped struct {
	raw  []byte
	size int64
}

// readCapped consumes the rest of the body, keeping at most max bytes.
func readCapped(br *bufio.Reader, max int) (capped, error) {
	var c capped
	buf := make([]byte, 32<<10)
	for {
		n, err := br.Read(buf)
		if n > 0 {
			c.size += int64(n)
			if room := max - len(c.raw); room > 0 {
				c.raw = append(c.raw, buf[:min(n, room)]...)
			}
		}
		if err == io.EOF {
			return c, nil
		}
		if err != nil {
			return c, err
		}
	}
}

// readElement reads one value: a container until its brackets balance, a
// string until its closing quote, or a scalar until a delimiter. It returns
// io.EOF when the body ended inside the element (ProblemTruncated).
func readElement(br *bufio.Reader, lim Limits) (Element, error) {
	var el Element
	keep := func(b byte) {
		el.Size++
		if len(el.Raw) < lim.MaxBytes {
			el.Raw = append(el.Raw, b)
		}
	}
	finish := func() Element {
		switch {
		case el.Size > int64(len(el.Raw)):
			el.Problem = ProblemTooLarge
		case el.Problem == "" && !utf8.Valid(el.Raw):
			el.Problem = ProblemInvalidUTF8
		}
		return el
	}

	depth, inString, escaped, scalar := 0, false, false, false
	start := true
	for {
		b, err := br.ReadByte()
		if err == io.EOF {
			if scalar && !inString {
				return finish(), io.EOF // a bare scalar at the very end: complete, the array is just unclosed
			}
			el = finish()
			el.Problem = ProblemTruncated
			return el, io.EOF
		}
		if err != nil {
			return el, err
		}
		if start {
			start = false
			switch b {
			case '{', '[':
			case '"':
				keep(b)
				inString = true
				continue
			default:
				scalar = true
			}
		}
		if scalar && !inString {
			if b == ',' || b == ']' || b == ' ' || b == '\t' || b == '\n' || b == '\r' {
				_ = br.UnreadByte()
				return finish(), nil
			}
			keep(b)
			continue
		}
		keep(b)
		if inString {
			switch {
			case escaped:
				escaped = false
			case b == '\\':
				escaped = true
			case b == '"':
				inString = false
				if depth == 0 { // a top-level string element is complete
					return finish(), nil
				}
			}
			continue
		}
		switch b {
		case '"':
			inString = true
		case '{', '[':
			depth++
			if depth > lim.MaxDepth {
				el.Problem = ProblemTooDeep
			}
		case '}', ']':
			depth--
			if depth <= 0 {
				return finish(), nil
			}
		}
	}
}
