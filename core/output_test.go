package core

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

const testTimeout = 10 * time.Second

func TestOutputReaderReadsFromStart(t *testing.T) {
	b := newOutputBuffer()
	b.Write([]byte("hello "))
	b.Write([]byte{0x00, 0xff})
	b.Close()

	// A reader created after all writes still sees everything from byte 0
	got, err := io.ReadAll(&outputReader{ctx: t.Context(), buf: b})
	if want := "hello \x00\xff"; err != nil || string(got) != want {
		t.Fatalf("ReadAll = %q, %v; want %q, nil", got, err, want)
	}
}

func TestOutputReaderWakesOnWriteAndClose(t *testing.T) {
	b := newOutputBuffer()
	r := &outputReader{ctx: t.Context(), buf: b}

	type result struct {
		data string
		err  error
	}
	read := func() <-chan result {
		res := make(chan result, 1)
		go func() {
			p := make([]byte, 16)
			n, err := r.Read(p)
			res <- result{string(p[:n]), err}
		}()
		return res
	}

	// assertBlocked checks that the Read is still waiting, so the next Write or Close must wake it
	assertBlocked := func(res <-chan result) {
		t.Helper()
		select {
		case got := <-res:
			t.Fatalf("Read returned %+v while no new output was available", got)
		case <-time.After(50 * time.Millisecond):
		}
	}

	res := read()
	assertBlocked(res)
	b.Write([]byte("x"))
	if got := mustRecv(t, res); got.err != nil || got.data != "x" {
		t.Fatalf("Read = %+v, want %q", got, "x")
	}

	res = read()
	assertBlocked(res)
	b.Close()
	if got := mustRecv(t, res); got.err != io.EOF {
		t.Fatalf("Read after Close error = %v, want io.EOF", got.err)
	}
}

func TestOutputReaderContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	r := &outputReader{ctx: ctx, buf: newOutputBuffer()}

	errc := make(chan error, 1)
	go func() {
		_, err := r.Read(make([]byte, 1))
		errc <- err
	}()
	cancel()
	if err := mustRecv(t, errc); !errors.Is(err, context.Canceled) {
		t.Fatalf("Read error = %v, want context.Canceled", err)
	}
}

func TestOutputReaderConcurrentReaders(t *testing.T) {
	b := newOutputBuffer()
	want := make([]byte, 1000)
	for i := range want {
		want[i] = byte(i)
	}

	type result struct {
		data []byte
		err  error
	}
	const readers = 10
	results := make(chan result, readers)
	for range readers {
		go func() {
			data, err := io.ReadAll(&outputReader{ctx: t.Context(), buf: b})
			results <- result{data, err}
		}()
	}
	for _, c := range want {
		b.Write([]byte{c})
	}
	b.Close()

	for i := range readers {
		if got := mustRecv(t, results); got.err != nil || !bytes.Equal(got.data, want) {
			t.Fatalf("reader %d got %d bytes, %v; want %d identical bytes, nil", i, len(got.data), got.err, len(want))
		}
	}
}

// mustRecv returns the next value from c, failing the test if none arrives within testTimeout
func mustRecv[T any](t *testing.T, c <-chan T) T {
	t.Helper()
	select {
	case v := <-c:
		return v
	case <-time.After(testTimeout):
	}
	t.Fatalf("nothing received within %v", testTimeout)
	var zero T
	return zero
}
