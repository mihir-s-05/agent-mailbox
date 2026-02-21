package model

import (
	"testing"
	"time"
)

func TestParsePriority(t *testing.T) {
	cases := []struct {
		name    string
		in      any
		want    int
		wantErr bool
	}{
		{"low enum", "LOW", 0, false},
		{"normal enum", "NORMAL", 1, false},
		{"high enum", "HIGH", 2, false},
		{"urgent enum", "URGENT", 3, false},
		{"int", 2, 2, false},
		{"float", 1.0, 1, false},
		{"float non-integer", 1.9, 0, true},
		{"bad string", "P1", 0, true},
		{"bad int", 9, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParsePriority(tc.in)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if !tc.wantErr && got != tc.want {
				t.Fatalf("got %d want %d", got, tc.want)
			}
		})
	}
}

func TestCursorEncodeDecode(t *testing.T) {
	now := time.Now().UTC().Round(time.Microsecond)
	orig := PollCursor{
		SnapshotAt:    now,
		LastPriority:  2,
		LastCreatedAt: now.Add(5 * time.Second),
		LastMessageID: "01TEST",
	}
	enc, err := EncodeCursor(orig)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	dec, err := DecodeCursor(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !dec.SnapshotAt.Equal(orig.SnapshotAt) || dec.LastPriority != orig.LastPriority || dec.LastMessageID != orig.LastMessageID || !dec.LastCreatedAt.Equal(orig.LastCreatedAt) {
		t.Fatalf("decoded mismatch: %#v vs %#v", dec, orig)
	}
}
