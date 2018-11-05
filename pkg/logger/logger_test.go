package logger

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"testing"
)

func TestLogger(t *testing.T) {
	ctx := context.Background()

	expected := "logger: trace=notrace hello"
	b := bytes.NewBufferString("")
	l := New(b, "logger", nil)
	l.Log(ctx, "hello")

	got, _ := ioutil.ReadAll(b)
	if expected != string(got) {
		t.Fatal(fmt.Sprintf("expected=%s got=%s", expected, string(got)))
	}

	expected = "logger: trace=logger logger 2"
	b = bytes.NewBufferString("")
	l = New(b, "logger", "logger")
	ctx = context.WithValue(ctx, "logger", "logger")
	l.Logf(ctx, "logger %d", 2)

	got, _ = ioutil.ReadAll(b)
	if expected != string(got) {
		t.Fatal(fmt.Sprintf("expected=%s got=%s", expected, string(got)))
	}

	// test stdout
	l = New(nil, "logger", nil)
}
