package redisqueue_test

import (
	"context"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// miniredis does not support Lua shebang metadata. This fixture adapter strips
// only that header; it does not emulate script flags or memory-pressure behavior.
type miniredisClient struct {
	*redis.Client
}

var _ redis.UniversalClient = (*miniredisClient)(nil)

func (c *miniredisClient) Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	return c.Client.Eval(ctx, miniredisLuaBody(script), keys, args...)
}

func miniredisLuaBody(script string) string {
	if strings.HasPrefix(script, "#!lua ") || strings.HasPrefix(script, "#!lua\n") {
		if _, body, ok := strings.Cut(script, "\n"); ok {
			return body
		}
	}
	return script
}

func TestMiniredisLuaBodyPreservesEverythingExceptLeadingHeader(t *testing.T) {
	body := "\n-- preserve whitespace and opaque bytes: \xff\x00\nreturn ARGV[1]\n"
	for _, tc := range []struct {
		name   string
		script string
		want   string
	}{
		{"plain", body, body},
		{"allow-oom", "#!lua flags=allow-oom\n" + body, body},
		{"other-metadata", "#!lua flags=no-writes,allow-oom\n" + body, body},
		{"bare-header", "#!lua\n" + body, body},
		{"empty-body", "#!lua flags=allow-oom\n", ""},
		{"unterminated-header", "#!lua flags=allow-oom", "#!lua flags=allow-oom"},
		{"not-leading", "\n#!lua flags=allow-oom\n" + body, "\n#!lua flags=allow-oom\n" + body},
		{"other-shebang", "#!/bin/sh\n" + body, "#!/bin/sh\n" + body},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, miniredisLuaBody(tc.script))
		})
	}
}

func TestMiniredisClientFlaggedScriptSHAFallbackPreservesKeysAndArguments(t *testing.T) {
	f := newTestQueue(t)
	ctx := context.Background()
	key, arg := "key:{opaque}\xff\x00", "argument:\xfe\x00"
	script := redis.NewScript("#!lua flags=allow-oom\nreturn {KEYS[1], ARGV[1], ARGV[2]}\n")
	// The original SHA need not exist: Script.Run must fall back through the
	// wrapper's Eval, including on subsequent runs of the flagged script.
	for range 2 {
		result, err := script.Run(ctx, f.client, []string{key}, arg, "last").Result()
		require.NoError(t, err)
		require.Equal(t, []any{key, arg, "last"}, result)
	}
}
