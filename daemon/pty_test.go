package daemon

import (
	"strings"
	"testing"

	"github.com/vito/midterm"
)

func TestRenderRowHTML_NullRunesAreSpaces(t *testing.T) {
	vt := midterm.NewTerminal(3, 10)
	// Fresh terminal has all-null content.
	sess := &ptySession{vt: vt}

	row := sess.renderRowHTML(0)
	// Must not contain U+FFFD or \x00.
	if strings.ContainsRune(row, '\x00') {
		t.Fatal("renderRowHTML produced null bytes in HTML")
	}
	if strings.ContainsRune(row, '\uFFFD') {
		t.Fatal("renderRowHTML produced U+FFFD replacement characters")
	}
	// The row should contain 10 spaces (the terminal width).
	if !strings.Contains(row, "          ") {
		t.Fatalf("expected 10 spaces for empty row, got: %q", row)
	}
}

func TestRenderRowHTML_MixedContentAndNulls(t *testing.T) {
	vt := midterm.NewTerminal(3, 10)
	// Write "Hi" at the start — remaining cells stay null.
	vt.Write([]byte("Hi"))

	sess := &ptySession{vt: vt}
	row := sess.renderRowHTML(0)

	if strings.ContainsRune(row, '\x00') {
		t.Fatal("renderRowHTML produced null bytes")
	}
	if !strings.Contains(row, "Hi") {
		t.Fatalf("expected 'Hi' in row, got: %q", row)
	}
	// The 8 trailing null cells should be spaces, not missing.
	if !strings.Contains(row, "        ") {
		t.Fatalf("expected 8 trailing spaces, got: %q", row)
	}
}

func TestRenderRowHTML_ClearScreen(t *testing.T) {
	vt := midterm.NewTerminal(3, 20)
	// Write some text, then clear the screen via ED2 (\e[2J).
	vt.Write([]byte("hello world"))
	vt.Write([]byte("\x1b[2J"))

	sess := &ptySession{vt: vt}
	row := sess.renderRowHTML(0)

	if strings.ContainsRune(row, '\x00') {
		t.Fatal("renderRowHTML produced null bytes after clear")
	}
	if strings.Contains(row, "hello") {
		t.Fatalf("screen clear did not remove content: %q", row)
	}
}
