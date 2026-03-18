package storage_test

import (
	"encoding/json"
	"testing"

	"github.com/semistrict/loophole/storage"
)

func TestCreateParamsUnmarshalStringSize(t *testing.T) {
	input := `{"volume":"test","size":"1073741824"}`
	var p storage.CreateParams
	if err := json.Unmarshal([]byte(input), &p); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if p.Volume != "test" {
		t.Fatalf("expected volume=test, got %s", p.Volume)
	}
	if p.Size != 1073741824 {
		t.Fatalf("expected size=1073741824, got %d", p.Size)
	}
}
