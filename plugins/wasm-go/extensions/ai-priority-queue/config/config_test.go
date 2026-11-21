package config

import (
	"testing"

	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm/proxytest"
	"github.com/stretchr/testify/assert"
	"github.com/tidwall/gjson"
)

// registerMockHost 注册 proxytest mock host：让 InitRedisClient 里的
// proxywasm.RedisInit 等主机调用走 no-op 实现（纯 Go 单测无完整 proxytest 环境，
// 仅借其 mock host 注册通道）。
func registerMockHost(t *testing.T) {
	t.Helper()
	_, release := proxytest.NewHostEmulator(proxytest.NewEmulatorOption())
	t.Cleanup(release)
}

// optionsFrom 解析除 Redis 连接外的全部可选项（不发起 host 调用）。
func optionsFrom(t *testing.T, jsonStr string) (*Config, error) {
	t.Helper()
	var cfg Config
	err := ParseOptions(gjson.Parse(jsonStr), &cfg)
	return &cfg, err
}

func TestParseOptions_Defaults(t *testing.T) {
	cfg, err := optionsFrom(t, `{}`)
	assert.NoError(t, err)
	assert.Equal(t, DefaultCacheTTLSeconds, cfg.TierCacheTTLSeconds)
	assert.Equal(t, DefaultMaxPollMilliseconds, cfg.MaxPollMilliseconds)
	assert.True(t, cfg.EnableRequestBodyInject)
}

func TestParseOptions_Custom(t *testing.T) {
	cfg, err := optionsFrom(t, `{
		"cache_ttl": 30,
		"max_poll_ms": 200,
		"request_body": false
	}`)
	assert.NoError(t, err)
	assert.Equal(t, 30, cfg.TierCacheTTLSeconds)
	assert.Equal(t, 200, cfg.MaxPollMilliseconds)
	assert.False(t, cfg.EnableRequestBodyInject)
}

func TestParseOptions_InvalidFallBackToDefaults(t *testing.T) {
	cfg, err := optionsFrom(t, `{
		"cache_ttl": -5,
		"max_poll_ms": 0
	}`)
	assert.NoError(t, err)
	assert.Equal(t, DefaultCacheTTLSeconds, cfg.TierCacheTTLSeconds)
	assert.Equal(t, DefaultMaxPollMilliseconds, cfg.MaxPollMilliseconds)
	assert.True(t, cfg.EnableRequestBodyInject)
}

func TestTierFromCode(t *testing.T) {
	for _, c := range []string{"P0", "P1", "P2", "P3", "p0", " p2 "} {
		code, ok := TierFromCode(c)
		assert.True(t, ok, "code %q should be valid", c)
		assert.NotEmpty(t, code)
	}
	for _, c := range []string{"", "P4", "PX", "p10"} {
		_, ok := TierFromCode(c)
		assert.False(t, ok, "code %q should be invalid", c)
	}
}

func TestEnginePriorityMapping(t *testing.T) {
	// xgs 终版映射：vLLM 数值越小越优先
	assert.Equal(t, -10, EnginePriority[TierP0])
	assert.Equal(t, -5, EnginePriority[TierP1])
	assert.Equal(t, 0, EnginePriority[TierP2])
	assert.Equal(t, 10, EnginePriority[TierP3])
	// 分档队列序号
	assert.Equal(t, 0, TierIndex[TierP0])
	assert.Equal(t, 1, TierIndex[TierP1])
	assert.Equal(t, 2, TierIndex[TierP2])
	assert.Equal(t, 3, TierIndex[TierP3])
}

func TestInitRedisClient_Validation(t *testing.T) {
	registerMockHost(t)

	var cfg Config
	// 缺 redis 段
	err := InitRedisClient(gjson.Parse(`{}`), &cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing redis")

	// 空 service_name
	err = InitRedisClient(gjson.Parse(`{"redis": {"service_name": ""}}`), &cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "service name must not be empty")

	// 合法：默认端口推断 + 字段透传
	cfg = Config{}
	err = InitRedisClient(gjson.Parse(`{"redis": {"service_name": "redis.dns"}}`), &cfg)
	assert.NoError(t, err)
	assert.Equal(t, 6379, cfg.ServicePort)
	assert.Equal(t, "redis.dns", cfg.ServiceName)

	cfg = Config{}
	err = InitRedisClient(gjson.Parse(`{"redis": {"service_name": "redis.static"}}`), &cfg)
	assert.NoError(t, err)
	assert.Equal(t, 80, cfg.ServicePort)

	cfg = Config{}
	err = InitRedisClient(gjson.Parse(`{
		"redis": {"service_name": "redis.dns", "service_port": 6380,
			"username": "u", "password": "p", "timeout": 2000, "database": 1}
	}`), &cfg)
	assert.NoError(t, err)
	assert.Equal(t, 6380, cfg.ServicePort)
	assert.Equal(t, 2000, cfg.Timeout)
	assert.Equal(t, 1, cfg.Database)
	assert.Equal(t, "u", cfg.Username)
	assert.Equal(t, "p", cfg.Password)
}

func TestParseConfig_FullConfig(t *testing.T) {
	registerMockHost(t)

	var cfg Config
	err := ParseConfig(gjson.Parse(`{
		"cache_ttl": 45,
		"max_poll_ms": 150,
		"request_body": true,
		"redis": {"service_name": "redis.dns", "service_port": 6379, "timeout": 1000}
	}`), &cfg)
	assert.NoError(t, err)
	assert.Equal(t, 45, cfg.TierCacheTTLSeconds)
	assert.Equal(t, 150, cfg.MaxPollMilliseconds)
	assert.True(t, cfg.EnableRequestBodyInject)
	assert.NotNil(t, cfg.RedisClient)

	// 缺 redis 段：整体解析失败
	err = ParseConfig(gjson.Parse(`{"cache_ttl": 45}`), &cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing redis")
}
