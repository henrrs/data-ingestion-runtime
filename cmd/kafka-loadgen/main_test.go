package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestBuildPayloadRepetitiveExactSize(t *testing.T) {
	t.Parallel()

	payload := buildPayload(7, 3, 512, "repetitive", 42)

	if got := len(payload); got != 512 {
		t.Fatalf("len(payload) = %d, want 512", got)
	}
	if !bytes.Contains(payload, []byte(`"pad":"xxxxx`)) {
		t.Fatalf("payload does not contain repetitive pad: %s", payload[:96])
	}
}

func TestBuildPayloadPseudoRandomDeterministic(t *testing.T) {
	t.Parallel()

	first := buildPayload(17, 4, 512, "pseudo-random", 12345)
	second := buildPayload(17, 4, 512, "pseudo-random", 12345)
	otherSequence := buildPayload(18, 4, 512, "pseudo-random", 12345)
	otherSeed := buildPayload(17, 4, 512, "pseudo-random", 54321)

	if !bytes.Equal(first, second) {
		t.Fatal("pseudo-random payload must be deterministic for the same sequence and seed")
	}
	if bytes.Equal(first, otherSequence) {
		t.Fatal("payload should change when sequence changes")
	}
	if bytes.Equal(first, otherSeed) {
		t.Fatal("payload should change when seed changes")
	}

	pad := extractPad(t, string(first))
	unique := make(map[rune]struct{})
	for _, ch := range pad {
		unique[ch] = struct{}{}
	}
	if len(unique) < 16 {
		t.Fatalf("expected high-entropy pad, got only %d unique chars", len(unique))
	}
}

func TestIsSupportedPayloadMode(t *testing.T) {
	t.Parallel()

	if !isSupportedPayloadMode("repetitive") {
		t.Fatal("repetitive should be supported")
	}
	if !isSupportedPayloadMode("pseudo-random") {
		t.Fatal("pseudo-random should be supported")
	}
	if isSupportedPayloadMode("random") {
		t.Fatal("random should not be supported")
	}
}

func extractPad(t *testing.T, payload string) string {
	t.Helper()

	prefix := `"pad":"`
	start := strings.Index(payload, prefix)
	if start == -1 {
		t.Fatalf("missing pad field in payload: %s", payload)
	}
	start += len(prefix)

	end := strings.LastIndex(payload, `"}`)
	if end == -1 || end < start {
		t.Fatalf("invalid payload suffix: %s", payload)
	}
	return payload[start:end]
}
