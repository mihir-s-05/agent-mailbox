package model

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"
)

const (
	DeliveryPending   = "PENDING"
	DeliveryDelivered = "DELIVERED"
	DeliveryAcked     = "ACKED"
	DeliveryExpired   = "EXPIRED"
	DeliveryCancelled = "CANCELLED"
)

const (
	ReadReceiptNone      = "NONE"
	ReadReceiptOnDeliver = "ON_DELIVER"
	ReadReceiptOnAck     = "ON_ACK"
)

type APIError struct {
	Code    string
	Message string
	Details map[string]any
}

func (e *APIError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func NewError(code, message string, details map[string]any) *APIError {
	return &APIError{Code: code, Message: message, Details: details}
}

func ParsePriority(v any) (int, error) {
	switch x := v.(type) {
	case int:
		return normalizePriority(x)
	case int32:
		return normalizePriority(int(x))
	case int64:
		n, err := intFromInt64(x)
		if err != nil {
			return 0, err
		}
		return normalizePriority(n)
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) || x != math.Trunc(x) {
			return 0, fmt.Errorf("priority must be an integer")
		}
		return normalizePriority(int(x))
	case float32:
		if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) || x != float32(math.Trunc(float64(x))) {
			return 0, fmt.Errorf("priority must be an integer")
		}
		return normalizePriority(int(x))
	case json.Number:
		n, err := x.Int64()
		if err != nil {
			return 0, fmt.Errorf("priority must be an integer")
		}
		asInt, convErr := intFromInt64(n)
		if convErr != nil {
			return 0, convErr
		}
		return normalizePriority(asInt)
	case string:
		switch strings.ToUpper(strings.TrimSpace(x)) {
		case "LOW":
			return 0, nil
		case "NORMAL":
			return 1, nil
		case "HIGH":
			return 2, nil
		case "URGENT":
			return 3, nil
		default:
			return 0, fmt.Errorf("unknown priority string %q", x)
		}
	default:
		return 0, fmt.Errorf("unsupported priority type %T", v)
	}
}

func intFromInt64(v int64) (int, error) {
	maxInt := int64(^uint(0) >> 1)
	minInt := -maxInt - 1
	if v < minInt || v > maxInt {
		return 0, fmt.Errorf("priority value out of range")
	}
	return int(v), nil
}

func normalizePriority(v int) (int, error) {
	if v < 0 || v > 3 {
		return 0, fmt.Errorf("priority out of range: %d", v)
	}
	return v, nil
}

func NormalizeReadReceipt(v string) (string, error) {
	s := strings.ToUpper(strings.TrimSpace(v))
	if s == "" {
		return ReadReceiptNone, nil
	}
	switch s {
	case ReadReceiptNone, ReadReceiptOnDeliver, ReadReceiptOnAck:
		return s, nil
	default:
		return "", fmt.Errorf("invalid read_receipt %q", v)
	}
}

type PollCursor struct {
	SnapshotAt    time.Time `json:"snapshot_at"`
	LastPriority  int       `json:"last_priority"`
	LastCreatedAt time.Time `json:"last_created_at"`
	LastMessageID string    `json:"last_message_id"`
}

func EncodeCursor(c PollCursor) (string, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func DecodeCursor(s string) (PollCursor, error) {
	var c PollCursor
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return c, err
	}
	err = json.Unmarshal(b, &c)
	return c, err
}

type Principal struct {
	Token   string
	TeamID  string
	Subject string
	Scopes  map[string]bool
}

func (p Principal) HasScope(scope string) bool {
	if p.Scopes["*"] {
		return true
	}
	return p.Scopes[scope]
}
