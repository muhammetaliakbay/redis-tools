package redistools_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"testing"
	"time"

	redistools "github.com/muhammetaliakbay/redis-tools"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func testClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
}

func randomString() string {
	claim := make([]byte, 16)
	_, err := rand.Read(claim)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(claim)
}

func TestLockerTryLock(t *testing.T) {
	client := testClient()
	locker := redistools.NewLocker(client)
	ctx := context.Background()
	key := randomString()
	expiration := time.Second * 2

	// lock should be claimed when it is free
	lockA, _, err := locker.TryLock(ctx, key, expiration)
	require.NoError(t, err)
	require.NotNil(t, lockA)

	halfExpiryTimer := time.NewTimer(expiration / 2)
	expiryTimer := time.NewTimer(expiration)

	// before expiry
	<-halfExpiryTimer.C

	// another lock should not be claimed
	lockB, _, err := locker.TryLock(ctx, key, expiration)
	require.ErrorIs(t, err, redistools.ErrUnclaimed)
	require.Nil(t, lockB)

	// lock should still be claimed
	err = lockA.Check(ctx)
	require.NoError(t, err)

	// on expiry
	<-expiryTimer.C

	// lock should be unclaimed
	err = lockA.Check(ctx)
	require.ErrorIs(t, err, redistools.ErrUnclaimed)
}

func TestLockerReset(t *testing.T) {
	client := testClient()
	locker := redistools.NewLocker(client)
	ctx := context.Background()
	key := randomString()
	expiration := time.Second * 2

	// lock should be claimed when it is free
	lockA, _, err := locker.TryLock(ctx, key, expiration)
	require.NoError(t, err)
	require.NotNil(t, lockA)

	halfExpiryTimer := time.NewTimer(expiration / 2)
	expiryTimer := time.NewTimer(expiration)

	// before expiry
	<-halfExpiryTimer.C

	// lock should still be claimed
	err = lockA.Check(ctx)
	require.NoError(t, err)

	// reset the lock
	err = lockA.Reset(ctx, expiration)
	require.NoError(t, err)

	resetHalfExpiryTimer := time.NewTimer(expiration / 2)
	resetExpiryTimer := time.NewTimer(expiration)

	// on expiry
	<-expiryTimer.C

	// lock should still be claimed
	err = lockA.Check(ctx)
	require.NoError(t, err)

	// before expiry
	<-resetHalfExpiryTimer.C

	// lock should still be claimed
	err = lockA.Check(ctx)
	require.NoError(t, err)

	// on expiry
	<-resetExpiryTimer.C

	// lock should be unclaimed
	err = lockA.Check(ctx)
	require.ErrorIs(t, err, redistools.ErrUnclaimed)
}

func TestLockerRelease(t *testing.T) {
	client := testClient()
	locker := redistools.NewLocker(client)
	ctx := context.Background()
	key := randomString()
	expiration := time.Second * 2

	// lock should be claimed when it is free
	lockA, _, err := locker.TryLock(ctx, key, expiration)
	require.NoError(t, err)
	require.NotNil(t, lockA)

	// release the lock
	err = lockA.Release(ctx)
	require.NoError(t, err)

	// lock should be unclaimed
	err = lockA.Check(ctx)
	require.ErrorIs(t, err, redistools.ErrUnclaimed)
}

func TestLockerLockAtExpiry(t *testing.T) {
	client := testClient()
	locker := redistools.NewLocker(client)
	ctx := context.Background()
	key := randomString()
	expiration := time.Second * 2
	tolerance := time.Millisecond * 100

	// lock should be claimed when it is free
	lockA, _, err := locker.TryLock(ctx, key, expiration)
	lockATime := time.Now()
	require.NoError(t, err)
	require.NotNil(t, lockA)

	lockB, err := locker.Lock(ctx, key, expiration)
	lockBTime := time.Now()
	require.WithinRange(t, lockBTime, lockATime.Add(expiration), lockATime.Add(expiration).Add(tolerance))
	require.NoError(t, err)
	require.NotNil(t, lockB)

	// lock should be claimed
	err = lockB.Check(ctx)
	require.NoError(t, err)

	// old lock should be unclaimed
	err = lockA.Check(ctx)
	require.ErrorIs(t, err, redistools.ErrUnclaimed)

	// release the lock
	err = lockB.Release(ctx)
	require.NoError(t, err)
}

func TestLockerLockAtRelease(t *testing.T) {
	client := testClient()
	locker := redistools.NewLocker(client)
	ctx := context.Background()
	key := randomString()
	expiration := time.Second * 2
	tolerance := time.Millisecond * 100

	// lock should be claimed when it is free
	lockA, _, err := locker.TryLock(ctx, key, expiration)
	require.NoError(t, err)
	require.NotNil(t, lockA)

	var releaseTime time.Time
	go func() {
		time.Sleep(expiration / 2)
		releaseTime = time.Now()
		err := lockA.Release(ctx)
		require.NoError(t, err)
	}()

	lockB, err := locker.Lock(ctx, key, expiration)
	lockTime := time.Now()
	require.WithinRange(t, lockTime, releaseTime, releaseTime.Add(tolerance))
	require.NoError(t, err)
	require.NotNil(t, lockB)

	// lock should be claimed
	err = lockB.Check(ctx)
	require.NoError(t, err)

	// old lock should be unclaimed
	err = lockA.Check(ctx)
	require.ErrorIs(t, err, redistools.ErrUnclaimed)

	// release the lock
	err = lockB.Release(ctx)
	require.NoError(t, err)
}

func TestLockerWithin(t *testing.T) {
	client := testClient()
	locker := redistools.NewLocker(client)
	ctx := context.Background()
	key := randomString()
	expiration := time.Second * 2

	// lock should be claimed when it is free
	lockA, _, err := locker.TryLock(ctx, key, expiration)
	require.NoError(t, err)
	require.NotNil(t, lockA)

	var releaseTime time.Time
	go func() {
		time.Sleep(expiration / 2)
		releaseTime = time.Now()
		err := lockA.Release(ctx)
		require.NoError(t, err)
	}()

	within := lockA.Within(ctx)
	<-within.Done()
	doneTime := time.Now()
	require.ErrorIs(t, context.Cause(within), redistools.ErrUnclaimed)
	require.WithinRange(t, doneTime, releaseTime, releaseTime.Add(time.Millisecond*100))

	// lock should be unclaimed
	err = lockA.Check(ctx)
	require.ErrorIs(t, err, redistools.ErrUnclaimed)
}
