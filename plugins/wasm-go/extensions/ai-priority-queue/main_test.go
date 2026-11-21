package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm/types"
	"github.com/higress-group/wasm-go/pkg/test"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/resp"
)

// basicConfig 基础配置（redis 走 proxytest mock host）
var basicConfig = func() json.RawMessage {
	data, _ := json.Marshal(map[string]interface{}{
		"cache_ttl":    60,
		"max_poll_ms":  100,
		"idle_poll_ms": 2000,
		"redis": map[string]interface{}{
			"service_name": "redis.static",
			"service_port": 6379,
			"timeout":      1000,
		},
	})
	return data
}()

// noInjectConfig 关闭 body 注入
var noInjectConfig = func() json.RawMessage {
	data, _ := json.Marshal(map[string]interface{}{
		"request_body": false,
		"redis": map[string]interface{}{
			"service_name": "redis.static",
			"service_port": 6379,
		},
	})
	return data
}()

// stringResp 构造 RESP 简单字符串应答（ADMIT/RELEASE 的返回均为字符串）
func stringResp(value string) []byte {
	b, err := resp.StringValue(value).MarshalRESP()
	if err != nil {
		panic(fmt.Sprintf("failed to marshal stringResp: %v", err))
	}
	return b
}

// bulkResp 构造 RESP 批量字符串应答（HGET tier 命中）
func bulkResp(value string) []byte {
	b, err := resp.BytesValue([]byte(value)).MarshalRESP()
	if err != nil {
		panic(fmt.Sprintf("failed to marshal bulkResp: %v", err))
	}
	return b
}

// callRedis 应答当前挂起的第一个 Redis 调用
func callRedis(t *testing.T, host test.TestHost, response []byte) {
	t.Helper()
	callouts := host.GetRedisCalloutAttributes()
	require.NotEmpty(t, callouts, "expected a pending redis call")
	host.CallOnRedisCall(0, response)
}

// resetPluginState 用例入口清空全部插件级状态（go 模式下 test.NewTestHost
// 复用同一 VM/插件状态，包级 map 跨测试残留会污染用例隔离）。
func resetPluginState() {
	resetTierCacheForTest()
	resetPollStateForTest()
}

// forcePollTick 把全部等待表项排到立即可派发后驱动一次 tick 扫表。
// 真实运行中下一拍由 100ms tick 底座按 nextDue 自然驱动；单测里时间不流逝，
// 故手动把 nextDue 归零（0 = 立即到期）模拟"到点"。
func forcePollTick() {
	for _, e := range pendingQueue {
		e.nextDue = 0
	}
	onPollTick()
}

// answerBusyProbe 应答一次忙闲预判的两拍探测（conf.load_threshold → pool.load_score），
// score 传空表示 HGET 返回 null（未知忙闲，走快档）。
func answerBusyProbe(t *testing.T, host test.TestHost, score string) {
	t.Helper()
	callRedis(t, host, test.CreateRedisRespNull()) // conf.load_threshold 缺失 → 默认 0.9
	if score == "" {
		callRedis(t, host, test.CreateRedisRespNull()) // pool.load_score 缺失 → 不忙
		return
	}
	callRedis(t, host, bulkResp(score))
}

func requestHeaders(credential, consumer string) [][2]string {
	headers := [][2]string{
		{":authority", "example.com"},
		{":path", "/v1/chat/completions"},
		{":method", "POST"},
		{"content-type", "application/json"},
	}
	if credential != "" {
		headers = append(headers, [2]string{"x-mse-credential", credential})
	}
	if consumer != "" {
		headers = append(headers, [2]string{"x-mse-consumer", consumer})
	}
	return headers
}

// 无认证身份：零成本直通，不发起任何 Redis 调用
func TestOnHttpRequestHeaders_NoIdentity_PassesWithoutRedis(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		resetPluginState()
		host, status := test.NewTestHost(basicConfig)
		defer host.Reset()
		require.Equal(t, types.OnPluginStartStatusOK, status)

		action := host.CallOnHttpRequestHeaders([][2]string{
			{":authority", "example.com"},
			{":path", "/v1/chat/completions"},
			{":method", "POST"},
		})
		require.Equal(t, types.ActionContinue, action)
		require.Len(t, host.GetRedisCalloutAttributes(), 0)
		host.CompleteHttp()
	})
}

// 档位命中（cred）：发起 HGET 查档位，未挂 AiRoute（无 service 维度）→ 只注入不排队
func TestOnHttpRequestHeaders_TierHit_NoRouteService_InjectOnly(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		resetPluginState()
		host, status := test.NewTestHost(basicConfig)
		defer host.Reset()
		require.Equal(t, types.OnPluginStartStatusOK, status)

		action := host.CallOnHttpRequestHeaders(requestHeaders("key-1", ""))
		require.Equal(t, types.HeaderStopAllIterationAndWatermark, action)

		// 1 次 Redis 调用：HGET pq:tier:cred:key-1 tier
		callouts := host.GetRedisCalloutAttributes()
		require.Len(t, callouts, 1)
		require.Contains(t, string(callouts[0].Query), "pq:tier:cred:key-1")

		// 命中 P2 → 无 service 维度直接放行
		callRedis(t, host, bulkResp("P2"))
		require.Equal(t, types.ActionContinue, host.GetHttpStreamAction())
		require.Nil(t, host.GetLocalResponse())

		// body 注入 priority=0（P2）
		action = host.CallOnHttpRequestBody([]byte(`{"model":"qwen","priority":99}`))
		require.Equal(t, types.ActionContinue, action)
		require.JSONEq(t, `{"model":"qwen","priority":0}`, string(host.GetRequestBody()))
		host.CompleteHttp()
	})
}

// 档位未命中且无 consumer：直接放行，不排队
func TestOnHttpRequestHeaders_TierMiss_Pass(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		resetPluginState()
		host, status := test.NewTestHost(basicConfig)
		defer host.Reset()
		require.Equal(t, types.OnPluginStartStatusOK, status)

		action := host.CallOnHttpRequestHeaders(requestHeaders("key-unknown", ""))
		require.Equal(t, types.HeaderStopAllIterationAndWatermark, action)
		require.Len(t, host.GetRedisCalloutAttributes(), 1)

		// HGET 返回 nil → 未命中且无 consumer → 放行
		callRedis(t, host, test.CreateRedisRespNull())
		require.Equal(t, types.ActionContinue, host.GetHttpStreamAction())
		require.Nil(t, host.GetLocalResponse())
		host.CompleteHttp()
	})
}

// cred 未命中回退 consumer 档位
func TestOnHttpRequestHeaders_TierFallbackToConsumer(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		resetPluginState()
		host, status := test.NewTestHost(basicConfig)
		defer host.Reset()
		require.Equal(t, types.OnPluginStartStatusOK, status)

		action := host.CallOnHttpRequestHeaders(requestHeaders("key-unknown", "u-42"))
		require.Equal(t, types.HeaderStopAllIterationAndWatermark, action)
		require.Len(t, host.GetRedisCalloutAttributes(), 1)

		// 第 1 拍：cred 未命中（nil）
		callRedis(t, host, test.CreateRedisRespNull())
		// 第 2 拍：user 档位命中 P0
		callouts := host.GetRedisCalloutAttributes()
		require.Len(t, callouts, 1)
		require.Contains(t, string(callouts[0].Query), "pq:tier:user:u-42")
		callRedis(t, host, bulkResp("P0"))

		// 无 service 维度 → 放行 + P0 注入
		require.Equal(t, types.ActionContinue, host.GetHttpStreamAction())
		action = host.CallOnHttpRequestBody([]byte(`{"model":"qwen"}`))
		require.Equal(t, types.ActionContinue, action)
		require.JSONEq(t, `{"model":"qwen","priority":-10}`, string(host.GetRequestBody()))
		host.CompleteHttp()
	})
}

// 有 service 维度（AiRoute 内部路由名）→ ADMIT 排队链
func TestOnHttpRequestHeaders_QueueChain_Admitted(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		resetPluginState()
		host, status := test.NewTestHost(basicConfig)
		defer host.Reset()
		require.Equal(t, types.OnPluginStartStatusOK, status)
		require.NoError(t, host.SetRouteName("ai-route-9a342114ba8a11ecb1bf00163e1250b5.internal"))

		action := host.CallOnHttpRequestHeaders(requestHeaders("key-1", ""))
		require.Equal(t, types.HeaderStopAllIterationAndWatermark, action)

		// 1. HGET 档位
		callouts := host.GetRedisCalloutAttributes()
		require.Len(t, callouts, 1)
		callRedis(t, host, bulkResp("P1"))

		// 2. ADMIT EVAL
		callouts = host.GetRedisCalloutAttributes()
		require.Len(t, callouts, 1)
		query := string(callouts[0].Query)
		require.Contains(t, query, "eval")
		require.Contains(t, query, "zadd") // ADMIT 脚本特征
		callRedis(t, host, stringResp("admitted"))

		// admitted → 放行
		require.Equal(t, types.ActionContinue, host.GetHttpStreamAction())
		require.Nil(t, host.GetLocalResponse())

		// body 注入 P1=-5
		action = host.CallOnHttpRequestBody([]byte(`{"model":"qwen","priority":7}`))
		require.Equal(t, types.ActionContinue, action)
		require.JSONEq(t, `{"model":"qwen","priority":-5}`, string(host.GetRequestBody()))

		// stream done → RELEASE EVAL（幂等释放）
		host.CompleteHttp()
		callouts = host.GetRedisCalloutAttributes()
		require.Len(t, callouts, 1)
		require.Contains(t, string(callouts[0].Query), "zrem") // RELEASE 脚本特征
	})
}

// waiting → 登记等待表 → tick 扫表续拍 → 再 ADMIT → admitted
func TestOnHttpRequestHeaders_QueueChain_WaitThenAdmit(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		resetPluginState()
		host, status := test.NewTestHost(basicConfig)
		defer host.Reset()
		require.Equal(t, types.OnPluginStartStatusOK, status)
		require.NoError(t, host.SetRouteName("ai-route-9a342114ba8a11ecb1bf00163e1250b5.internal"))

		action := host.CallOnHttpRequestHeaders(requestHeaders("key-1", ""))
		require.Equal(t, types.HeaderStopAllIterationAndWatermark, action)

		callRedis(t, host, bulkResp("P2"))        // 档位
		callRedis(t, host, stringResp("waiting")) // 第 1 拍 ADMIT：waiting
		// waiting 后异步探判忙闲（2 拍）：conf.load_threshold 缺失 → 默认 0.9，
		// pool.load_score=0.5 < 0.9 → 不忙，保持快档 max_poll_ms
		answerBusyProbe(t, host, "0.5")

		// 等待表登记 1 条，未派发在途
		require.Len(t, pendingQueue, 1)

		// tick 扫表续拍：ADMIT EVAL
		forcePollTick()
		callouts := host.GetRedisCalloutAttributes()
		require.Len(t, callouts, 1)
		query := string(callouts[0].Query)
		require.Contains(t, query, "eval")
		require.Contains(t, query, "zadd") // ADMIT 脚本特征（无 EXISTS 探测）
		require.NotContains(t, query, "exists")

		// 第 2 拍 ADMIT：admitted
		callRedis(t, host, stringResp("admitted"))
		require.Len(t, pendingQueue, 0) // admitted 摘表
		require.Equal(t, types.ActionContinue, host.GetHttpStreamAction())

		action = host.CallOnHttpRequestBody([]byte(`{"model":"qwen"}`))
		require.Equal(t, types.ActionContinue, action)
		require.JSONEq(t, `{"model":"qwen","priority":0}`, string(host.GetRequestBody()))
		host.CompleteHttp()
	})
}

// 卡忙闲（load_score ≥ load_threshold）→ 慢档 idle_poll_ms：到点不立即续拍
func TestOnHttpRequestHeaders_QueueChain_BusyUsesIdlePollInterval(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		resetPluginState()
		host, status := test.NewTestHost(basicConfig)
		defer host.Reset()
		require.Equal(t, types.OnPluginStartStatusOK, status)
		require.NoError(t, host.SetRouteName("ai-route-9a342114ba8a11ecb1bf00163e1250b5.internal"))

		action := host.CallOnHttpRequestHeaders(requestHeaders("key-1", ""))
		require.Equal(t, types.HeaderStopAllIterationAndWatermark, action)

		callRedis(t, host, bulkResp("P2"))        // 档位
		callRedis(t, host, stringResp("waiting")) // 第 1 拍 ADMIT：waiting
		// 忙闲预判：load_score=0.95 ≥ 默认阈值 0.9 → 忙 → 慢档 2000ms
		answerBusyProbe(t, host, "0.95")

		require.Len(t, pendingQueue, 1)
		for _, e := range pendingQueue {
			require.True(t, e.nextDue > time.Now().UnixMilli(), "busy waiter should be deferred to idle_poll_ms")
		}

		// nextDue 未到（模拟 2000ms 内的 tick）：不派发 ADMIT
		onPollTick()
		require.Len(t, host.GetRedisCalloutAttributes(), 0)
		require.Len(t, pendingQueue, 1)
		host.CompleteHttp()
	})
}

// full → 429 + x-ai-drop-reason: saturated
func TestOnHttpRequestHeaders_QueueChain_Full_Rejected429(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		resetPluginState()
		host, status := test.NewTestHost(basicConfig)
		defer host.Reset()
		require.Equal(t, types.OnPluginStartStatusOK, status)
		require.NoError(t, host.SetRouteName("ai-route-9a342114ba8a11ecb1bf00163e1250b5.internal"))

		action := host.CallOnHttpRequestHeaders(requestHeaders("key-1", ""))
		require.Equal(t, types.HeaderStopAllIterationAndWatermark, action)

		callRedis(t, host, bulkResp("P3"))
		callRedis(t, host, stringResp("full"))

		localResp := host.GetLocalResponse()
		require.NotNil(t, localResp)
		require.Equal(t, uint32(http.StatusTooManyRequests), localResp.StatusCode)
		require.Equal(t, "saturated", headerValue(localResp.Headers, "x-ai-drop-reason"))
		host.CompleteHttp()
	})
}

// ttl → 429 + x-ai-drop-reason: ttl-expired
func TestOnHttpRequestHeaders_QueueChain_Ttl_Rejected429(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		resetPluginState()
		host, status := test.NewTestHost(basicConfig)
		defer host.Reset()
		require.Equal(t, types.OnPluginStartStatusOK, status)
		require.NoError(t, host.SetRouteName("ai-route-9a342114ba8a11ecb1bf00163e1250b5.internal"))

		action := host.CallOnHttpRequestHeaders(requestHeaders("key-1", ""))
		require.Equal(t, types.HeaderStopAllIterationAndWatermark, action)

		callRedis(t, host, bulkResp("P2"))
		callRedis(t, host, stringResp("ttl"))

		localResp := host.GetLocalResponse()
		require.NotNil(t, localResp)
		require.Equal(t, uint32(http.StatusTooManyRequests), localResp.StatusCode)
		require.Equal(t, "ttl-expired", headerValue(localResp.Headers, "x-ai-drop-reason"))
		host.CompleteHttp()
	})
}

// disabled → 只注入不排队
func TestOnHttpRequestHeaders_QueueChain_Disabled_InjectOnly(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		resetPluginState()
		host, status := test.NewTestHost(basicConfig)
		defer host.Reset()
		require.Equal(t, types.OnPluginStartStatusOK, status)
		require.NoError(t, host.SetRouteName("ai-route-9a342114ba8a11ecb1bf00163e1250b5.internal"))

		action := host.CallOnHttpRequestHeaders(requestHeaders("key-1", ""))
		require.Equal(t, types.HeaderStopAllIterationAndWatermark, action)

		callRedis(t, host, bulkResp("P2"))
		callRedis(t, host, stringResp("disabled"))

		require.Equal(t, types.ActionContinue, host.GetHttpStreamAction())
		require.Nil(t, host.GetLocalResponse())

		action = host.CallOnHttpRequestBody([]byte(`{"model":"qwen"}`))
		require.Equal(t, types.ActionContinue, action)
		require.JSONEq(t, `{"model":"qwen","priority":0}`, string(host.GetRequestBody()))
		host.CompleteHttp()
	})
}

// Redis 故障（EVAL error 应答）→ fail-open 放行
func TestOnHttpRequestHeaders_RedisError_FailOpen(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		resetPluginState()
		host, status := test.NewTestHost(basicConfig)
		defer host.Reset()
		require.Equal(t, types.OnPluginStartStatusOK, status)
		require.NoError(t, host.SetRouteName("ai-route-9a342114ba8a11ecb1bf00163e1250b5.internal"))

		action := host.CallOnHttpRequestHeaders(requestHeaders("key-1", ""))
		require.Equal(t, types.HeaderStopAllIterationAndWatermark, action)

		callRedis(t, host, bulkResp("P2"))
		// ADMIT EVAL 报错
		callRedis(t, host, test.CreateRedisRespError("ERR redis down"))

		require.Equal(t, types.ActionContinue, host.GetHttpStreamAction())
		require.Nil(t, host.GetLocalResponse())
		host.CompleteHttp()
	})
}

// 未命中档位无注入：body 原样保留（客户端自带 priority 不被改写也不被信任）
func TestOnHttpRequestBody_NoTier_NoInject(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		resetPluginState()
		host, status := test.NewTestHost(basicConfig)
		defer host.Reset()
		require.Equal(t, types.OnPluginStartStatusOK, status)

		action := host.CallOnHttpRequestHeaders(requestHeaders("key-unknown", ""))
		require.Equal(t, types.HeaderStopAllIterationAndWatermark, action)
		callRedis(t, host, test.CreateRedisRespNull())
		require.Equal(t, types.ActionContinue, host.GetHttpStreamAction())

		action = host.CallOnHttpRequestBody([]byte(`{"model":"qwen","priority":99}`))
		require.Equal(t, types.ActionContinue, action)
		// 无档位：客户端值不动（本插件不读不信任，但也不覆盖无 tier 流量）
		require.JSONEq(t, `{"model":"qwen","priority":99}`, string(host.GetRequestBody()))
		host.CompleteHttp()
	})
}

// request_body=false：不注入
func TestOnHttpRequestBody_InjectDisabled(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		resetPluginState()
		host, status := test.NewTestHost(noInjectConfig)
		defer host.Reset()
		require.Equal(t, types.OnPluginStartStatusOK, status)

		action := host.CallOnHttpRequestHeaders(requestHeaders("key-1", ""))
		require.Equal(t, types.HeaderStopAllIterationAndWatermark, action)
		callRedis(t, host, bulkResp("P0"))
		require.Equal(t, types.ActionContinue, host.GetHttpStreamAction())

		action = host.CallOnHttpRequestBody([]byte(`{"model":"qwen","priority":99}`))
		require.Equal(t, types.ActionContinue, action)
		require.JSONEq(t, `{"model":"qwen","priority":99}`, string(host.GetRequestBody()))
		host.CompleteHttp()
	})
}

// sjsonSetPriority 单测：覆盖/新增/空对象/含 priority 的原始字节覆盖
func TestSjsonSetPriority(t *testing.T) {
	// 无 priority：尾部插入
	out, err := sjsonSetPriority([]byte(`{"model":"qwen","temperature":0.5}`), -10)
	require.NoError(t, err)
	require.JSONEq(t, `{"model":"qwen","temperature":0.5,"priority":-10}`, string(out))

	// 已有 priority：原位覆盖
	out, err = sjsonSetPriority([]byte(`{"model":"qwen","priority":99}`), 10)
	require.NoError(t, err)
	require.JSONEq(t, `{"model":"qwen","priority":10}`, string(out))

	// 空对象
	out, err = sjsonSetPriority([]byte(`{}`), 0)
	require.NoError(t, err)
	require.JSONEq(t, `{"priority":0}`, string(out))

	// 嵌套对象中的 priority 不受影响，只覆盖顶层
	out, err = sjsonSetPriority(
		[]byte(`{"model":"qwen","priority":99,"extra":{"priority":1}}`), -5)
	require.NoError(t, err)
	require.JSONEq(t, `{"model":"qwen","priority":-5,"extra":{"priority":1}}`, string(out))
}

func headerValue(headers [][2]string, key string) string {
	for _, h := range headers {
		if strings.EqualFold(h[0], key) {
			return h[1]
		}
	}
	return ""
}
