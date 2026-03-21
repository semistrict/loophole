package env

import "testing"

func TestHasOption(t *testing.T) {
	t.Setenv("LOOPHOLE_OPTIONS", "foo, nonbd bar;BAZ requirenbd")
	if !HasOption("nonbd") {
		t.Fatal("expected nonbd option to be present")
	}
	if !HasOption("requirenbd") {
		t.Fatal("expected requirenbd option to be present")
	}
	if !HasOption("baz") {
		t.Fatal("expected baz option to be present")
	}
	if HasOption("missing") {
		t.Fatal("did not expect missing option to be present")
	}
}
