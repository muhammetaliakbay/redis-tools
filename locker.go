package redistools

import (
	"context"
	"time"

	"errors"

	"github.com/redis/go-redis/v9"
)

var ErrUnclaimed = errors.New("lock is unclaimed")

const LOCK_RETRY_PERIOD = time.Second * 5
const WATCH_PERIOD = time.Second * 5

type Locker struct {
	client *redis.Client
}

func NewLocker(
	client *redis.Client,
) *Locker {
	return &Locker{
		client: client,
	}
}

func (l *Locker) TryLock(
	ctx context.Context,
	key string,
	expiration time.Duration,
) (*Lock, error) {
	var claim string
	err := l.client.Watch(ctx, func(tx *redis.Tx) error {
		owner := tx.Get(ctx, key)
		if owner.Err() == nil {
			return ErrUnclaimed
		}
		if !errors.Is(owner.Err(), redis.Nil) {
			return owner.Err()
		}
		claim = randomClaim()
		pipe := tx.TxPipeline()
		pipe.Set(ctx, key, claim, expiration)
		pipe.Publish(ctx, key, claim)
		_, err := pipe.Exec(ctx)
		return err
	}, key)
	if errors.Is(err, redis.TxFailedErr) {
		return nil, ErrUnclaimed
	}
	if err != nil {
		return nil, err
	}
	return &Lock{
		locker: l,
		key:    key,
		claim:  claim,
	}, nil
}

func (l *Locker) Lock(
	ctx context.Context,
	key string,
	expiration time.Duration,
) (*Lock, error) {
	var pubsub *redis.PubSub
	ticker := time.NewTicker(LOCK_RETRY_PERIOD)
	defer ticker.Stop()
	for {
		lock, err := l.TryLock(ctx, key, expiration)
		if !errors.Is(err, ErrUnclaimed) {
			return lock, err
		}

		if pubsub == nil {
			pubsub = l.client.Subscribe(ctx, key)
			defer pubsub.Close()
		}

		select {
		case <-ticker.C:
		case <-pubsub.ChannelWithSubscriptions():
		}

	}
}

type Lock struct {
	locker *Locker
	key    string
	claim  string
}

func (l *Lock) Reset(
	ctx context.Context,
	expiration time.Duration,
) error {
	for {
		err := l.locker.client.Watch(
			ctx,
			func(tx *redis.Tx) error {
				owner := tx.Get(ctx, l.key)
				if errors.Is(owner.Err(), redis.Nil) {
					return ErrUnclaimed
				}
				if owner.Err() != nil {
					return owner.Err()
				}
				if owner.Val() != l.claim {
					return ErrUnclaimed
				}
				pipe := tx.TxPipeline()
				pipe.Expire(ctx, l.key, expiration)
				pipe.Publish(ctx, l.key, l.claim)
				_, err := pipe.Exec(ctx)
				return err
			},
			l.key,
		)
		if errors.Is(err, redis.TxFailedErr) {
			continue
		}
		return err
	}
}

func (l *Lock) Release(
	ctx context.Context,
) error {
	for {
		err := l.locker.client.Watch(
			ctx,
			func(tx *redis.Tx) error {
				owner := tx.Get(ctx, l.key)
				if errors.Is(owner.Err(), redis.Nil) {
					return ErrUnclaimed
				}
				if owner.Err() != nil {
					return owner.Err()
				}
				if owner.Val() != l.claim {
					return ErrUnclaimed
				}
				pipe := tx.TxPipeline()
				pipe.Del(ctx, l.key)
				pipe.Publish(ctx, l.key, l.claim)
				_, err := pipe.Exec(ctx)
				return err
			},
			l.key,
		)
		if errors.Is(err, redis.TxFailedErr) {
			continue
		}
		return err
	}
}

func (l *Lock) Check(
	ctx context.Context,
) error {
	owner := l.locker.client.Get(ctx, l.key)
	if errors.Is(owner.Err(), redis.Nil) {
		return ErrUnclaimed
	}
	if owner.Err() != nil {
		return owner.Err()
	}
	if owner.Val() != l.claim {
		return ErrUnclaimed
	}
	return nil
}

func (l *Lock) CheckRemaining(
	ctx context.Context,
) (time.Duration, error) {
	pipe := l.locker.client.TxPipeline()
	owner := pipe.Get(ctx, l.key)
	ttl := pipe.TTL(ctx, l.key)
	pipe.Exec(ctx)

	if errors.Is(owner.Err(), redis.Nil) {
		return 0, ErrUnclaimed
	}
	if owner.Err() != nil {
		return 0, owner.Err()
	}
	if owner.Val() != l.claim {
		return 0, ErrUnclaimed
	}

	return ttl.Val(), nil
}

func (l *Lock) Within(outerCtx context.Context) context.Context {
	ctx, cancel := context.WithCancelCause(outerCtx)
	go func() {
		var timer *time.Timer
		var ticker *time.Ticker
		var pubsub *redis.PubSub
		defer func() {
			if timer != nil {
				timer.Stop()
			}
		}()
		for {
			remaining, err := l.CheckRemaining(ctx)
			if err != nil {
				cancel(err)
				return
			}

			if timer != nil {
				timer.Stop()
			}
			timer = time.NewTimer(remaining)

			if ticker == nil {
				ticker = time.NewTicker(WATCH_PERIOD)
				defer ticker.Stop()
				pubsub = l.locker.client.Subscribe(ctx, l.key)
				defer pubsub.Close()
			}

			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			case <-timer.C:
			case <-pubsub.ChannelWithSubscriptions():
			}
		}
	}()
	return ctx
}
