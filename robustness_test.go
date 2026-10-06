package redisqueue_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-hypercube/go-hypercube/queue"
	redisqueue "github.com/go-hypercube/hypercube-queue-redis"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestOpaqueStringFieldsPreserveNonUTF8Bytes(t *testing.T) {
	f := newTestQueue(t)
	ctx := context.Background()
	original := &queue.Message{
		ID: "id:\xff\x00", QueueName: "queue:\xfe\x00",
		Namespace: "namespace:\xfd", JobName: "job:\xfc",
		Payload: []byte{}, Extra: nil,
	}
	require.NoError(t, f.q.Push(ctx, original))
	first := popOne(t, f.q, original.QueueName)
	require.Equal(t, original.ID, first.ID)
	require.Equal(t, original.QueueName, first.QueueName)
	require.Equal(t, original.Namespace, first.Namespace)
	require.Equal(t, original.JobName, first.JobName)
	require.NotNil(t, first.Payload)
	require.Empty(t, first.Payload)
	require.Nil(t, first.Extra)
	require.NoError(t, f.q.DeadLetter(ctx, first))
	n, err := f.q.Redrive(ctx, original.QueueName, 1)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	second := popOne(t, f.q, original.QueueName)
	require.Equal(t, original.ID, second.ID)
	require.Equal(t, original.Namespace, second.Namespace)
	require.Equal(t, original.JobName, second.JobName)
	require.NoError(t, f.q.Ack(ctx, second))
}

func TestSubmillisecondVisibilityDoesNotBecomeTheDefault(t *testing.T) {
	for _, duration := range []time.Duration{-time.Nanosecond, time.Nanosecond} {
		t.Run(duration.String(), func(t *testing.T) {
			f := newTestQueue(t)
			f.freezeTime()
			ctx := context.Background()
			require.NoError(t, f.q.Push(ctx, &queue.Message{QueueName: "jobs", VisibilityTimeout: duration}))
			delivery := popOne(t, f.q, "jobs")
			if duration > 0 {
				requireStats(t, f.q, queue.QueueStats{Name: "jobs", InFlight: 1})
				f.advanceTime(time.Millisecond)
			}
			requireStats(t, f.q, queue.QueueStats{Name: "jobs", Ready: 1})
			require.ErrorIs(t, f.q.ExtendVisibility(ctx, time.Second, delivery), queue.ErrNotFound)
			requireBatchFailures(t, f.q.Ack(ctx, delivery), []*queue.Message{delivery}, map[int]error{0: queue.ErrNotFound})
		})
	}
}

func TestRedriveCommandReplayDoesNotExceedRequestedCount(t *testing.T) {
	for _, count := range []int{1, 129} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			f := newTestQueue(t)
			ctx := context.Background()
			input := make([]*queue.Message, count+10)
			for i := range input {
				input[i] = &queue.Message{QueueName: "jobs"}
			}
			require.NoError(t, f.q.Push(ctx, input...))
			deliveries := popExactly(t, f.q, "jobs", len(input))
			require.NoError(t, f.q.DeadLetter(ctx, deliveries...))
			// Simulate a command being executed twice with the same arguments,
			// as can happen after go-redis loses a response and retries it.
			q := redisqueue.New(&replayingClient{UniversalClient: f.client}, redisqueue.WithKeyPrefix(f.prefix))
			n, err := q.Redrive(ctx, "jobs", count)
			require.NoError(t, err)
			require.Equal(t, count, n)
			requireStats(t, f.q, queue.QueueStats{Name: "jobs", Ready: int64(count), Dead: 10})
		})
	}
}

type replayingClient struct {
	redis.UniversalClient
}

func (c *replayingClient) Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	result := c.UniversalClient.Eval(ctx, script, keys, args...)
	if result.Err() != nil {
		return result
	}
	return c.UniversalClient.Eval(ctx, script, keys, args...)
}

func (c *replayingClient) EvalSha(ctx context.Context, sha string, keys []string, args ...any) *redis.Cmd {
	result := c.UniversalClient.EvalSha(ctx, sha, keys, args...)
	if result.Err() != nil {
		return result
	}
	return c.UniversalClient.EvalSha(ctx, sha, keys, args...)
}

func TestCancellationStopsWaitingForActiveBackendCalls(t *testing.T) {
	for _, operation := range []string{"Push", "Pop", "Ack", "Retry", "DeadLetter", "ExtendVisibility", "Stats", "Queues", "Redrive"} {
		t.Run(operation, func(t *testing.T) {
			f := newTestQueue(t)
			ctx := context.Background()
			require.NoError(t, f.q.Push(ctx, &queue.Message{QueueName: "jobs"}, &queue.Message{QueueName: "jobs"}))
			deliveries := popExactly(t, f.q, "jobs", 2)
			require.NoError(t, f.q.DeadLetter(ctx, deliveries[1]))
			require.NoError(t, f.q.Push(ctx, &queue.Message{QueueName: "jobs"}))
			// Deliberately use a client without context-based socket deadlines.
			// The driver still must return promptly when the caller cancels.
			client := redis.NewClient(&redis.Options{
				Addr: f.addr, MaxRetries: -1, ContextTimeoutEnabled: false,
				ReadTimeout: time.Second, WriteTimeout: time.Second,
			})
			t.Cleanup(func() { _ = client.Close() })
			q := redisqueue.New(client, redisqueue.WithKeyPrefix(f.prefix), redisqueue.WithPollingTimeout(0))
			hook := &pausedCommandHook{started: make(chan struct{}), resume: make(chan struct{})}
			client.AddHook(hook)
			defer close(hook.resume)
			call := func(ctx context.Context) error {
				switch operation {
				case "Push":
					return q.Push(ctx, &queue.Message{QueueName: "jobs"})
				case "Pop":
					_, err := q.Pop(ctx, "jobs", 1)
					return err
				case "Ack":
					return q.Ack(ctx, deliveries[0])
				case "Retry":
					return q.Retry(ctx, deliveries[0])
				case "DeadLetter":
					return q.DeadLetter(ctx, deliveries[0])
				case "ExtendVisibility":
					return q.ExtendVisibility(ctx, time.Minute, deliveries[0])
				case "Stats":
					_, err := q.Stats(ctx, "jobs")
					return err
				case "Queues":
					_, err := q.Queues(ctx)
					return err
				default:
					_, err := q.Redrive(ctx, "jobs", 1)
					return err
				}
			}
			canceled, cancel := context.WithCancel(ctx)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- call(canceled) }()
			select {
			case <-hook.started:
			case <-time.After(time.Second):
				t.Fatal("operation never reached Redis client")
			}
			cancel()
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("operation waited for backend after context cancellation")
			}
		})
	}
}

type pausedCommandHook struct {
	started chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func (h *pausedCommandHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *pausedCommandHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.once.Do(func() {
			close(h.started)
			<-h.resume
		})
		return next(ctx, cmd)
	}
}

func (h *pausedCommandHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
