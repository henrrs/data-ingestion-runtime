package kafka

import "testing"

func TestExtractSchemaID(t *testing.T) {
	value := []byte{0, 0, 0, 0, 25, 1, 2, 3}
	if got := extractSchemaID(value); got != 25 {
		t.Fatalf("expected schema id 25, got %d", got)
	}
}

func TestExtractSchemaIDWithoutWireFormat(t *testing.T) {
	value := []byte(`{"id":1}`)
	if got := extractSchemaID(value); got != 0 {
		t.Fatalf("expected schema id 0, got %d", got)
	}
}
