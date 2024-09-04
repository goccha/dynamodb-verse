package batches

import (
	"time"
)

type batchOption struct {
	maxRetry    int
	interval    time.Duration
	maxInterval time.Duration
}

func defaultBatchOption() batchOption {
	return batchOption{
		maxRetry:    3,
		interval:    time.Second,
		maxInterval: time.Minute,
	}
}

type Option func(*batchOption) *batchOption

func MaxRetry(maxRetry int) Option {
	return func(input *batchOption) *batchOption {
		if input != nil {
			input.maxRetry = maxRetry
		}
		return input
	}
}

func RetryInterval(maxInterval, interval time.Duration) Option {
	return func(input *batchOption) *batchOption {
		if input != nil {
			input.interval = interval
			input.maxInterval = maxInterval
		}
		return input
	}
}
