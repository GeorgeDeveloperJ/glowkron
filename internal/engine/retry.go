package engine

import (
	"crypto/rand"
	"math/big"
	"time"
)

// CalculateBackoff computes the exponential backoff duration for a given attempt
// with randomized jitter in the [80%, 120%) window to prevent thundering herd bursts.
// attempt is 1-indexed (attempt 1 uses baseDelay).
// If maxDelay > 0, the resulting delay is capped at maxDelay.
func CalculateBackoff(attempt int, baseDelay time.Duration, maxDelay time.Duration) time.Duration {
	if baseDelay <= 0 {
		return 0
	}
	if attempt <= 0 {
		attempt = 1
	}

	// Cap exponent to prevent overflow (1 << 30 is ~1 billion)
	exp := attempt - 1
	if exp > 30 {
		exp = 30
	}

	factor := time.Duration(1 << exp)
	delay := baseDelay * factor

	// Guard against potential multiplication overflow
	if delay < 0 || (factor > 0 && delay/factor != baseDelay) {
		if maxDelay > 0 {
			delay = maxDelay
		} else {
			delay = 1 << 62
		}
	}

	if maxDelay > 0 && delay > maxDelay {
		delay = maxDelay
	}

	// Jitter: random multiplier in [0.8, 1.2)
	jitterMultiplier := 0.8 + 0.4*cryptoRandFloat()
	backoff := time.Duration(float64(delay) * jitterMultiplier)

	if maxDelay > 0 && backoff > maxDelay {
		backoff = maxDelay
	}
	if backoff < 0 {
		backoff = 0
	}

	return backoff
}

func cryptoRandFloat() float64 {
	n, err := rand.Int(rand.Reader, big.NewInt(1_000_000))
	if err != nil {
		return 0.5
	}
	return float64(n.Int64()) / 1_000_000.0
}
