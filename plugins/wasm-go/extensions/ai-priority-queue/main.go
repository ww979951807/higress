// Copyright (c) 2024-2025
// Beijing Baolande Software Corporation
//
// ai-priority-queue: 优先级染色 + Redis 分级排队准入 + body priority 注入。
//
// COMPUTING-7814 数据面插件（Higress wasm-go 形态），语义对照 xgs 终版
// APISIX Lua 插件（origin/release/2.2.0-ALPHA4.0-005-1130-xgs），全部重新
// 实现、未 cherry-pick。准入判定复用 xgs 终版 ADMIT/RELEASE Lua 语义：
//
//   - 严格优先级 ahead 不等式（同级更早等待者 + 所有更高 tier 等待者）
//   - 同级 FCFS（等待队列 score = 请求截止时间 ms）
//   - 分级硬上限 q{tier} 超限 → full（429 saturated）
//   - 等待超过 tier TTL → ttl（429 ttl-expired）
//   - P0 预留 R = floor(C × p0_reserve_pct%)，低优先级在途上限 C - R
//   - P1..P3 min_rps{tier}×replicas 令牌防饿死（仅本档队首可越过高档积压）
//   - 可选全局 max_admit_rps 令牌（0=关闭）
//   - 租约 TTL 脚本内夹紧 180s（防孤儿租约占满 C）
//   - 分值缺失/unknown/过期 → waiting（留在队列由 TTL 兜底，不旁路）
//   - Redis 故障/服务未启用/档位缺失 → fail-open：只注入不排队
//
// 与 APISIX 的实现差异（wasm 无阻塞原语）：
// 等待用"登记表 + tick 扫表"实现——onHttpRequestHeaders 返回
// HeaderStopAllIterationAndWatermark 暂停请求，ADMIT 返回 waiting 时把请求
// 登记进插件级等待表；wrapper.RegisterTickFunc（100ms 底座）每拍扫表，对
// 到期表项先 proxywasm.SetEffectiveContext 切回请求上下文再发 ADMIT EVAL
// （SDK 的 redis 回调按派发时刻的生效上下文路由，故回调可安全操作该请求）。
// 轮询节奏分档：卡忙闲（load_score ≥ load_threshold）用 idle_poll_ms
// （默认 2000ms，对齐控制面忙闲分 5s 更新周期），卡名额/未知用 max_poll_ms
// （默认 100ms），避免百级排队者以 Redis 往返为节拍把 Redis 打爆。
// admitted / full / ttl / disabled / fail-open / 断连（ProcessStreamDone）
// 均移出登记表；表项超限告警并单拍限流；在途回调失联超时回收。
package main

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm/types"
	"github.com/higress-group/wasm-go/pkg/log"
	"github.com/higress-group/wasm-go/pkg/wrapper"
	"github.com/tidwall/gjson"
	"github.com/tidwall/resp"

	"ai-priority-queue/config"
	"ai-priority-queue/util"
)

const (
	pluginName = "ai-priority-queue"

	// tierKeyPrefix Redis 档位映射前缀（控制面 push_priority_tier 写入）：
	// HASH pq:tier:cred:{api_key} field=tier / HASH pq:tier:user:{user_id} field=tier
	tierCredPrefix = "pq:tier:cred:"
	tierUserPrefix = "pq:tier:user:"
	// tierField 档位 HASH 的 field 名
	tierField = "tier"

	// loadScoreField pool HASH 中忙闲分 field（控制面 sync_admission_scores 写入）
	loadScoreField = "load_score"
	// loadThresholdField conf HASH 中准入阈值 field
	loadThresholdField = "load_threshold"
	// defaultLoadThreshold load_threshold 字段缺失/非法时的兜底值（与 ADMIT 脚本一致）
	defaultLoadThreshold = 0.9

	// busyProbeTTLMS 服务忙闲预判缓存时长，对齐控制面 sync_admission_scores 的
	// 5s 周期：忙闲分本身 5s 才变一次，预判结果短期内不会失效
	busyProbeTTLMS = 5000
	// thresholdCacheTTLMS load_threshold 缓存时长（准入门限属低频配置）
	thresholdCacheTTLMS = 60000

	// pendingWarnLimit 等待登记表告警阈值
	pendingWarnLimit = 10000
	// maxDispatchPerTick 单拍最多派发的 ADMIT EVAL 数（表项超限时强制节流，
	// 派发速率上限 = maxDispatchPerTick × 10/s，防止扫表打爆 Redis）
	maxDispatchPerTick = 128
	// stuckDispatchTimeoutMS 在途 EVAL 回收阈值：回调远超 Redis timeout 仍未
	// 返回，说明宿主已删除请求上下文（回调永不触发），强制摘除防泄漏
	stuckDispatchTimeoutMS = 30000
)

// ctxKeys 请求上下文键
const (
	ctxTier       = "aiPriorityQueueTier"
	ctxTierIdx    = "aiPriorityQueueTierIdx"
	ctxServiceID  = "aiPriorityQueueServiceID"
	ctxQueueState = "aiPriorityQueueQueueState"
	ctxPendingKey = "aiPriorityQueuePendingKey"
	// ctxHttpContextID HTTP 上下文 ID，由 contextCapturingPluginCtx 在
	// NewHttpContext 时写入；tick 扫表唤醒（SetEffectiveContext）依赖它
	ctxHttpContextID = "aiPriorityQueueCtxID"
)

// queueState 准入判定状态机（headers 阶段异步链的中间态）
type queueState int

const (
	stateIdle    queueState = iota // 未进入排队（无 service 维度/无 tier/未启用）
	stateWaiting                   // EVAL 返回 waiting，已登记等待表由 tick 驱动
	stateDone                      // admitted / disabled / full / ttl / error，链终止
)

// queueContext headers 阶段排队链上下文
type queueContext struct {
	serviceID string
	tier      config.TierCode
	tierIdx   int
	requestID string
	deadline  int64 // 等待截止时间 ms
	prefix    string
	round     int
}

// pendingEntry 等待登记表表项：一个正在排队的挂起请求
type pendingEntry struct {
	ctx          wrapper.HttpContext
	ctxID        uint32         // HTTP 上下文 ID（SetEffectiveContext 唤醒用）
	cfg          *config.Config // 登记时的配置快照（RedisClient 随之固定）
	qc           *queueContext
	nextDue      int64 // 下次允许派发 ADMIT 的时间 ms（0 = 立即到期）
	inFlight     bool  // 已派发 EVAL、等待回调
	dispatchedAt int64 // 最近一次派发时间 ms（在途超时回收用）
}

// busyCacheEntry 服务忙闲预判缓存
type busyCacheEntry struct {
	busy    bool
	expires int64
}

// thresholdCacheEntry 服务准入阈值缓存
type thresholdCacheEntry struct {
	value   float64
	expires int64
}

// ADMIT_SCRIPT 准入脚本（numkeys=1，语义对照 xgs 终版）。
//
//	KEYS[1] = <prefix>active        （单节点无 cluster，仍按 numkeys=1 保留键族约定）
//	ARGV[1] = prefix "pq:{svc}:"
//	ARGV[2] = tierIdx 0..3
//	ARGV[3] = requestId
//	ARGV[4] = nowMs
//	ARGV[5] = deadlineMs（等待截止时间）
//
// 返回: disabled / full / ttl / admitted / waiting
const ADMIT_SCRIPT = `
local P = ARGV[1]
local tier = tonumber(ARGV[2])
local now = tonumber(ARGV[4])
local member = tier .. ':' .. ARGV[3]
local wkey = P .. 'w:' .. tier
local active = KEYS[1]
local wd, seqK, confK, poolK, leaseK =
    P .. 'wd', P .. 'seq', P .. 'conf', P .. 'pool', P .. 'lease'

-- 0. 顺手清理: 全局租约 + 分档在途 + 过期等待
--    等待队列 score 即该请求的截止时间(ms)，过期成员可被直接清除；
--    即使 wd 索引缺失/损坏也不会留下无截止时间的僵尸队首。
redis.call('zremrangebyscore', active, '-inf', now)
for t = 0, 3 do
    redis.call('zremrangebyscore', P .. 'active:' .. t, '-inf', now)
    redis.call('zremrangebyscore', P .. 'w:' .. t, '-inf', now)
end
-- wd 兼容清理: 历史成员按 seq 计分，仍需按 wd 过期
local stale = redis.call('zrangebyscore', wd, '-inf', now)
for _, m in ipairs(stale) do
    local t = tonumber(string.sub(m, 1, 1))
    redis.call('zrem', P .. 'w:' .. t, m)
end
if #stale > 0 then redis.call('zremrangebyscore', wd, '-inf', now) end

-- 1. 开关
if redis.call('hget', confK, 'enabled') ~= '1' then return 'disabled' end

-- 2. 入队(仅首次): score 取截止时间，FCFS 排序与过期清理共用同一分值
if not redis.call('zscore', wkey, member) then
    local maxq = tonumber(redis.call('hget', confK, 'q' .. tier) or '0')
    if maxq > 0 and redis.call('zcard', wkey) >= maxq then
        return 'full'
    end
    redis.call('incr', seqK)
    redis.call('zadd', wkey, ARGV[5], member)
    redis.call('zadd', wd, ARGV[5], member)
elseif not redis.call('zscore', wd, member) then
    -- 自愈: 成员已在队列但 wd 索引缺失(历史部分写入)，补回截止时间
    redis.call('zadd', wd, ARGV[5], member)
end

-- 3. 等待 TTL
if now >= tonumber(ARGV[5]) then
    redis.call('zrem', wkey, member)
    redis.call('zrem', wd, member)
    return 'ttl'
end

-- 4. 分值新鲜度: 缺失/unknown/过期 -> 留在队列等待, 由 TTL 兜底。
--    不旁路, 避免分值不可用时绕过 load_threshold。
local staleness = tonumber(redis.call('hget', confK, 'staleness_ms') or '10000')
local score = tonumber(redis.call('hget', poolK, 'load_score'))
local scoreTs = tonumber(redis.call('hget', poolK, 'load_updated_ms') or '0')
local unknown = redis.call('hget', poolK, 'unknown')
if score == nil or unknown == '1' or scoreTs == 0
        or now - scoreTs > staleness then
    return 'waiting'
end

-- 5. ahead = 同级更早的 + 所有更高 tier 等待者（zrank 兜底 0 防 nil 算术错误）
--    pos 单独保留: aging 只放本档队首, 避免后到请求越过同档 FCFS。
local pos = redis.call('zrank', wkey, member) or 0
local ahead = pos
for t = 0, tier - 1 do
    ahead = ahead + redis.call('zcard', P .. 'w:' .. t)
end

-- 6. C = replicas × capacity_per_replica(单副本基数来自 concurrency_budget,
--    字段缺失时兜底 256, 与 max-num-seqs 对齐)。
--    P0 预留 R = floor(C × p0_reserve_pct%), 低优先级最多使用 C - R。
local replicas = tonumber(redis.call('hget', poolK, 'replicas') or '1') or 1
if replicas < 1 then replicas = 1 end
local cpr = tonumber(redis.call('hget', confK, 'capacity_per_replica') or '256') or 256
if cpr < 1 then cpr = 1 end
local C = replicas * cpr
local pct = tonumber(redis.call('hget', confK, 'p0_reserve_pct') or '0') or 0
if pct < 0 then pct = 0 elseif pct > 100 then pct = 100 end
local lowCap = C - math.floor(C * pct / 100)
if lowCap < 0 then lowCap = 0 end

local total, totalLow = 0, 0
for t = 0, 3 do
    local n = redis.call('zcard', P .. 'active:' .. t)
    total = total + n
    if t > 0 then totalLow = totalLow + n end
end

local function admit()
    redis.call('zrem', wkey, member)
    redis.call('zrem', wd, member)
    local leaseTtl = tonumber(redis.call('hget', confK, 'lease_ttl_ms') or '600000')
    -- 兜底夹紧: 即使库中仍配 600s, 网关也不会让孤儿租约占满 C 10 分钟
    if leaseTtl > 180000 then leaseTtl = 180000 end
    redis.call('zadd', active, now + leaseTtl, member)
    redis.call('zadd', P .. 'active:' .. tier, now + leaseTtl, member)
    redis.call('hset', leaseK, member, now)
    redis.call('hincrby', poolK, 'reserved', 1)
    return 'admitted'
end

-- 令牌桶: rate 为"每副本 RPS × replicas"(每秒令牌数), 突发上限 1。
-- 状态存放在 pq:{svc}:pool 的 tb:<name>:tokens / tb:<name>:ts 字段。
local function take_token(name, rate)
    if rate <= 0 then return false end
    local tkField = 'tb:' .. name .. ':tokens'
    local tsField = 'tb:' .. name .. ':ts'
    local tokens = tonumber(redis.call('hget', poolK, tkField))
    if tokens == nil then tokens = 1.0 end
    local ts = tonumber(redis.call('hget', poolK, tsField) or '0') or 0
    if ts <= 0 or now < ts then ts = now end
    tokens = tokens + (now - ts) * rate / 1000
    if tokens > 1.0 then tokens = 1.0 end
    if tokens < 1.0 then
        redis.call('hset', poolK, tkField, string.format('%.6f', tokens))
        redis.call('hset', poolK, tsField, now)
        return false
    end
    redis.call('hset', poolK, tkField, string.format('%.6f', tokens - 1.0))
    redis.call('hset', poolK, tsField, now)
    return true
end

local threshold = tonumber(redis.call('hget', confK, 'load_threshold') or '0.9') or 0.9

-- 7. P0: 独立阈值(缺省沿用 load_threshold), 可占用全部 C, 仍受全局在途上限约束。
if tier == 0 then
    local p0Threshold = tonumber(redis.call('hget', confK, 'p0_load_threshold') or '') or threshold
    if ahead == 0 and total < C and score < p0Threshold then
        return admit()
    end
    return 'waiting'
end

-- 8. P1..P3 常规准入: 轮到我 + 全局未满 + 低优先级未超 C-R + 低于阈值,
--    并受可选全局速率上限 max_admit_rps(0=关闭)约束。
local maxAdmit = tonumber(redis.call('hget', confK, 'max_admit_rps') or '0') or 0
local globalRate = maxAdmit * replicas
local function global_ok()
    return globalRate <= 0 or take_token('all', globalRate)
end
if ahead == 0 and total < C and totalLow < lowCap and score < threshold
        and global_ok() then
    return admit()
end

-- 9. P1..P3 最低放行速率: 本档队首(不越过同档 FCFS), 按 min_rps[tier]×replicas
--    独立取令牌, 允许越过更高档积压, 防止低优先级饿死。仍受 C/C-R/阈值/全局速率约束。
if pos == 0 and total < C and totalLow < lowCap and score < threshold then
    local rate = tonumber(redis.call('hget', confK, 'min_rps' .. tier) or '0') or 0
    if rate > 0 and global_ok() and take_token('tier' .. tier, rate * replicas) then
        return admit()
    end
end
return 'waiting'
`

// RELEASE_SCRIPT 释放脚本（numkeys=1，幂等，ProcessStreamDone 兜底调用，
// 客户端断连同样会触发 stream done）。
//
//	KEYS[1] = <prefix>active ; ARGV[1] = prefix, ARGV[2] = tierIdx, ARGV[3] = requestId
const RELEASE_SCRIPT = `
local P = ARGV[1]
local tier = tonumber(ARGV[2])
local member = tier .. ':' .. ARGV[3]
if redis.call('hget', P .. 'lease', member) then
    local r = tonumber(redis.call('hget', P .. 'pool', 'reserved') or '0')
    if r > 0 then redis.call('hincrby', P .. 'pool', 'reserved', -1) end
end
redis.call('hdel', P .. 'lease', member)
redis.call('zrem', KEYS[1], member)
redis.call('zrem', P .. 'active:' .. tier, member)
redis.call('zrem', P .. 'w:' .. tier, member)
redis.call('zrem', P .. 'wd', member)
return 1
`

func main() {}

// contextCapturingVMCtx / contextCapturingPluginCtx 上下文 ID 捕获层。
//
// 框架（wrapper）的 CommonHttpCtx.contextID 是未导出字段，SDK 也没有任何
// "取当前 HTTP 上下文 ID"的宿主调用；而 tick 扫表唤醒挂起请求必须调用
// proxywasm.SetEffectiveContext(httpCtxID)（SDK 文档明确该宿主调用服务于
// OnTick/OnQueueReady 场景，且 redis 回调按派发时刻的生效上下文路由）。
// 因此在 init() 处包一层 VM/Plugin 上下文：仅拦截 NewHttpContext 把
// contextID 写进请求上下文，其余全部提升自 wrapper 既有实现，不影响框架
// 对 headers/body/stream-done/tick 的路由。
type contextCapturingVMCtx struct {
	*wrapper.CommonVmCtx[config.Config]
}

func (v *contextCapturingVMCtx) NewPluginContext(contextID uint32) types.PluginContext {
	base := v.CommonVmCtx.NewPluginContext(contextID).(*wrapper.CommonPluginCtx[config.Config])
	return &contextCapturingPluginCtx{CommonPluginCtx: base}
}

type contextCapturingPluginCtx struct {
	*wrapper.CommonPluginCtx[config.Config]
}

func (p *contextCapturingPluginCtx) NewHttpContext(contextID uint32) types.HttpContext {
	httpCtx := p.CommonPluginCtx.NewHttpContext(contextID)
	if hctx, ok := httpCtx.(*wrapper.CommonHttpCtx[config.Config]); ok {
		hctx.SetContext(ctxHttpContextID, contextID)
	}
	return httpCtx
}

func init() {
	proxywasm.SetVMContext(&contextCapturingVMCtx{
		CommonVmCtx: wrapper.NewCommonVmCtx[config.Config](
			pluginName,
			wrapper.ParseConfig(parseConfig),
			wrapper.ProcessRequestHeaders(onHttpRequestHeaders),
			wrapper.ProcessRequestBody(onHttpRequestBody),
			wrapper.ProcessStreamDone(onProcessStreamDone),
		),
	})
}

// tierCache 档位映射进程内缓存（cred/consumer → {code, expires}）。
// 注意：wasm-go 插件按 (plugin, request) 双级 goroutine 模型运行，
// headers/body 阶段在同一请求上下文中串行执行；进程级 map 只在
// headers 阶段读写，单 worker 内无并发写，无需加锁（与 ai-quota 的
// credential2Name 进程级缓存同口径）。
var tierCache = map[string]tierCacheEntry{}

type tierCacheEntry struct {
	code    config.TierCode
	ok      bool
	expires int64
}

// 等待登记表 / 忙闲预判缓存（插件级，进程内）。
// proxy-wasm 事件循环在同一 worker 内串行执行请求回调、Redis 回调与 tick，
// 故与 tierCache 同口径不加锁。
var (
	pendingQueue   = map[string]*pendingEntry{}
	busyCache      = map[string]busyCacheEntry{}
	thresholdCache = map[string]thresholdCacheEntry{}
	// activeCfg 最近一次 parseConfig 生效的配置（tick 扫表用）
	activeCfg *config.Config
	// tickRegistered 防止 config reload 重复注册 tick（RegisterTickFunc 是追加语义）
	tickRegistered bool
	// overflowWarned 表项超限告警去重（回到阈值以下后重新武装）
	overflowWarned bool
)

// resetTierCacheForTest 仅测试使用：go 模式下 test.NewTestHost 复用同一
// VM/插件状态，包级 tierCache 跨测试残留会污染用例隔离，故每个用例入口清空。
func resetTierCacheForTest() {
	tierCache = map[string]tierCacheEntry{}
}

// resetPollStateForTest 仅测试使用：清空等待登记表与忙闲预判缓存。
func resetPollStateForTest() {
	pendingQueue = map[string]*pendingEntry{}
	busyCache = map[string]busyCacheEntry{}
	thresholdCache = map[string]thresholdCacheEntry{}
	overflowWarned = false
}

func parseConfig(json gjson.Result, cfg *config.Config) error {
	if err := config.ParseConfig(json, cfg); err != nil {
		return err
	}
	activeCfg = cfg
	ensureTickRegistered()
	return nil
}

// ensureTickRegistered 注册 tick 扫表函数（100ms 底座，须在 parseConfig 阶段
// 注册；各表项的实际节奏由 nextDue 控制，与 tick 底座解耦）。
func ensureTickRegistered() {
	if tickRegistered {
		return
	}
	tickRegistered = true
	wrapper.RegisterTickFunc(100, onPollTick)
}

// pendingKey 等待表键：service 维度 + 请求唯一 ID
func pendingKey(serviceID, requestID string) string {
	return serviceID + "|" + requestID
}

func registerPending(key string, e *pendingEntry) {
	pendingQueue[key] = e
}

func lookupPending(key string) *pendingEntry {
	return pendingQueue[key]
}

// removePending 摘除表项，返回是否存在
func removePending(key string) bool {
	if _, ok := pendingQueue[key]; ok {
		delete(pendingQueue, key)
		return true
	}
	return false
}

// onHttpRequestHeaders 执行链第 1 段：认头 + 查档位 + 准入判定。
//
// 档位来源（防伪造，客户端传入的优先级头/body 一律不读）：
//  1. X-Mse-Credential（key-auth 注入 apikey）→ HASH pq:tier:cred:{api_key}
//  2. 未命中回退 X-Mse-Consumer（用户 id）→ HASH pq:tier:user:{user_id}
//  3. 都未命中 → 无档位，直接放行（未启用优先级的流量零成本直通）
func onHttpRequestHeaders(ctx wrapper.HttpContext, cfg config.Config) types.Action {
	ctx.DisableReroute()

	credential := util.GetRequestHeader(util.CredentialHeader)
	consumer := util.GetRequestHeader(util.ConsumerHeader)
	if credential == "" && consumer == "" {
		// 无认证身份（key-auth 未挂/未命中）：零成本直通
		return types.ActionContinue
	}

	log.Debugf("%s: headers, credential=%q consumer=%q", pluginName, credential, consumer)

	// 档位为异步 Redis 查询：先回 HeaderStopAllIterationAndWatermark，
	// 查到 tier 后在回调里继续准入判定或直接放行。
	ctx.SetContext(ctxTier, config.TierCode(""))
	resolveTier(ctx, &cfg, credential, consumer, func(tier config.TierCode, ok bool) {
		if !ok {
			// 档位缺失：fail-open，只注入不排队
			ctx.SetContext(ctxQueueState, stateDone)
			proxywasm.ResumeHttpRequest()
			return
		}
		ctx.SetContext(ctxTier, tier)
		ctx.SetContext(ctxTierIdx, config.TierIndex[tier])
		startQueueChain(ctx, &cfg)
	})
	return types.HeaderStopAllIterationAndWatermark
}

// resolveTier 查档位（带 60s 进程内缓存）。cred 优先，未命中回退 user。
// Redis 故障按 fail-open 处理：视为未命中。
func resolveTier(ctx wrapper.HttpContext, cfg *config.Config, credential, consumer string, cb func(tier config.TierCode, ok bool)) {
	now := time.Now().UnixMilli()
	ttlMS := int64(cfg.TierCacheTTLSeconds) * 1000

	if credential != "" {
		if entry, hit := tierCache[credential]; hit && entry.expires > now {
			cb(entry.code, entry.ok)
			return
		}
		cfg.RedisClient.HGet(tierCredPrefix+credential, tierField, func(response resp.Value) {
			code, ok := tierFromResponse(response)
			if ok {
				tierCache[credential] = tierCacheEntry{code: code, ok: true, expires: now + ttlMS}
				cb(code, true)
				return
			}
			// cred 未命中 → 回退 user（未启用优先级的用户档为空）
			if consumer == "" {
				tierCache[credential] = tierCacheEntry{ok: false, expires: now + ttlMS}
				cb("", false)
				return
			}
			resolveUserTier(cfg, consumer, now, ttlMS, cb)
		})
		return
	}
	if consumer != "" {
		resolveUserTier(cfg, consumer, now, ttlMS, cb)
		return
	}
	cb("", false)
}

func resolveUserTier(cfg *config.Config, consumer string, now, ttlMS int64, cb func(tier config.TierCode, ok bool)) {
	if entry, hit := tierCache[consumer]; hit && entry.expires > now {
		cb(entry.code, entry.ok)
		return
	}
	cfg.RedisClient.HGet(tierUserPrefix+consumer, tierField, func(response resp.Value) {
		code, ok := tierFromResponse(response)
		tierCache[consumer] = tierCacheEntry{code: code, ok: ok, expires: now + ttlMS}
		cb(code, ok)
	})
}

func tierFromResponse(response resp.Value) (config.TierCode, bool) {
	if response.Error() != nil || response.IsNull() {
		return "", false
	}
	code := response.String()
	if strings.TrimSpace(code) == "" {
		return "", false
	}
	return config.TierFromCode(code)
}

// startQueueChain 启动准入判定链。无 service 维度（models 路由/聚合路由）
// 或 Redis 不可用时只注入不排队。
func startQueueChain(ctx wrapper.HttpContext, cfg *config.Config) {
	tier, _ := ctx.GetContext(ctxTier).(config.TierCode)
	tierIdx, _ := ctx.GetContext(ctxTierIdx).(int)
	serviceID := util.ServiceIDFromRouteName(util.GetRouteName())
	ctx.SetContext(ctxServiceID, serviceID)

	if serviceID == "" {
		// 无 service 维度：只做 body 注入，不排队
		ctx.SetContext(ctxQueueState, stateDone)
		proxywasm.ResumeHttpRequest()
		return
	}

	if !cfg.RedisClient.Ready() {
		// Redis 未就绪：fail-open（只注入不排队，不放大队列）
		log.Warnf("%s: redis not ready, fail-open, service=%s", pluginName, serviceID)
		ctx.SetContext(ctxQueueState, stateDone)
		proxywasm.ResumeHttpRequest()
		return
	}

	ctxID, _ := ctx.GetContext(ctxHttpContextID).(uint32)
	if ctxID == 0 {
		// 上下文 ID 缺失（理论上不可达）：tick 无法唤醒，按 fail-open 处理
		log.Errorf("%s: http context id unavailable, fail-open, service=%s", pluginName, serviceID)
		ctx.SetContext(ctxQueueState, stateDone)
		proxywasm.ResumeHttpRequest()
		return
	}

	requestID := requestIDForContext()
	now := time.Now().UnixMilli()
	qc := &queueContext{
		serviceID: serviceID,
		tier:      tier,
		tierIdx:   tierIdx,
		requestID: requestID,
		deadline:  now + defaultTierTTLMS(tierIdx),
		prefix:    fmt.Sprintf("pq:{%s}:", serviceID),
	}
	key := pendingKey(serviceID, requestID)
	ctx.SetContext(ctxQueueState, stateWaiting)
	ctx.SetContext("aiPriorityQueueCtx", qc)
	ctx.SetContext(ctxPendingKey, key)
	// body 注入需要缓冲完整 body，且 headers 已暂停；请求恢复后
	// wrapper 会自动进入 body 阶段（onHttpRequestBody）。
	registerPending(key, &pendingEntry{ctx: ctx, ctxID: ctxID, cfg: cfg, qc: qc})
	dispatchAdmitRound(key)
}

// defaultTierTTLMS 各 tier 默认等待 TTL（xgs 终版 DEFAULT_TIER_TTL）；Redis
// conf.t{tier} 可覆盖。首拍无法读 conf（脚本内每次都会校验 deadline），因此
// 这里用默认值生成 deadline，实际过期仍由脚本按 ARGV[5] 判定。
func defaultTierTTLMS(tierIdx int) int64 {
	switch tierIdx {
	case 0:
		return 180 * 1000
	default:
		return 360 * 1000
	}
}

// requestIDForContext 请求级唯一 ID（Envoy x-request-id 优先）。
func requestIDForContext() string {
	if raw, err := proxywasm.GetProperty([]string{"x_request_id"}); err == nil && len(raw) > 0 {
		return string(raw)
	}
	return fmt.Sprintf("wasm-%d", time.Now().UnixNano())
}

// dispatchAdmitRound 派发一轮 ADMIT EVAL。
//
// 先 SetEffectiveContext 切回请求上下文再派发：SDK 的 redis 回调按"派发时刻
// 的生效上下文"路由（vmstate 记录 callerContextID 并在响应到达时恢复），
// 因此无论本轮由请求链还是 tick 扫表发起，回调都能安全操作该请求
// （SetContext / ResumeHttpRequest / SendHttpResponse）。
func dispatchAdmitRound(key string) {
	e := lookupPending(key)
	if e == nil {
		return
	}
	e.inFlight = true
	e.dispatchedAt = time.Now().UnixMilli()

	if err := proxywasm.SetEffectiveContext(e.ctxID); err != nil {
		// 上下文已被宿主删除：无法唤醒，静默摘除（stream done 已清理过资源）
		log.Errorf("%s: set effective context failed, drop pending, service=%s member=%d:%s: %v",
			pluginName, e.qc.serviceID, e.qc.tierIdx, e.qc.requestID, err)
		removePending(key)
		return
	}

	qc := e.qc
	args := []interface{}{
		qc.prefix, qc.tierIdx, qc.requestID,
		time.Now().UnixMilli(), qc.deadline,
	}
	err := e.cfg.RedisClient.Eval(ADMIT_SCRIPT, 1,
		[]interface{}{qc.prefix + "active"}, args, func(response resp.Value) {
			handleAdmitResponse(key, response)
		})
	if err != nil {
		// 调度失败：fail-open
		log.Errorf("%s: admit dispatch failed: %v", pluginName, err)
		finishPendingResume(key)
	}
}

// handleAdmitResponse 处理一轮 ADMIT 结果（运行于该请求的生效上下文）。
func handleAdmitResponse(key string, response resp.Value) {
	e := lookupPending(key)
	if e == nil {
		// 表项已被摘除（断连/超限回收），结果作废
		return
	}
	ctx, qc := e.ctx, e.qc

	if err := response.Error(); err != nil {
		// Redis 故障：fail-open（只注入不排队）
		log.Errorf("%s: admit eval error: %v", pluginName, err)
		finishPendingResume(key)
		return
	}
	switch response.String() {
	case "admitted":
		ctx.SetContext(ctxQueueState, stateDone)
		ctx.SetContext("aiPriorityQueueAdmitted", true)
		finishPendingResume(key)
	case "disabled":
		// 服务未启用排队：只注入
		ctx.SetContext(ctxQueueState, stateDone)
		finishPendingResume(key)
	case "full":
		ctx.SetContext(ctxQueueState, stateDone)
		finishPendingReject(key, "saturated",
			"priority queue for this tier is full, retry later")
	case "ttl":
		ctx.SetContext(ctxQueueState, stateDone)
		finishPendingReject(key, "ttl-expired",
			"queue wait exceeded tier ttl")
	case "waiting":
		e.inFlight = false
		qc.round++
		if time.Now().UnixMilli() >= qc.deadline {
			ctx.SetContext(ctxQueueState, stateDone)
			finishPendingReject(key, "ttl-expired",
				"queue wait exceeded tier ttl")
			return
		}
		scheduleNextFromBusy(e.cfg, key, e)
	default:
		log.Errorf("%s: unexpected admit result: %q", pluginName, response.String())
		ctx.SetContext(ctxQueueState, stateDone)
		finishPendingResume(key)
	}
}

// finishPendingResume 链终止（放行类结果）：摘表 + 恢复请求。
func finishPendingResume(key string) {
	e := lookupPending(key)
	if e == nil {
		return
	}
	removePending(key)
	proxywasm.ResumeHttpRequest()
}

// finishPendingReject 链终止（拒绝类结果）：摘表 + 429。
func finishPendingReject(key, reason, message string) {
	e := lookupPending(key)
	if e == nil {
		return
	}
	removePending(key)
	reject(http.StatusTooManyRequests, reason, message)
}

// scheduleNextFromBusy 依据服务忙闲预判排定下一拍。
//
// 先按快档（max_poll_ms）落位，随后异步探判忙闲：命中慢档（卡忙闲）则把
// nextDue 推远到 idle_poll_ms。探判结果带 5s 缓存（对齐控制面忙闲分更新
// 周期），绝大多数轮次零额外 Redis 开销。
func scheduleNextFromBusy(cfg *config.Config, key string, e *pendingEntry) {
	e.nextDue = time.Now().UnixMilli() + int64(cfg.MaxPollMilliseconds)
	decideServiceBusy(cfg, e.qc.serviceID, e.qc.prefix, func(busy bool) {
		cur := lookupPending(key)
		if cur == nil || cur != e || cur.inFlight {
			// 表项已摘除/已派发：探判结果作废
			return
		}
		if busy {
			cur.nextDue = time.Now().UnixMilli() + int64(cfg.IdlePollMilliseconds)
		}
	})
}

// decideServiceBusy 服务忙闲预判（带缓存）：load_score ≥ load_threshold 视为忙。
// 分值缺失/unknown/解析失败一律按"不忙"处理（未知场景走快档，由脚本判定兜底）。
func decideServiceBusy(cfg *config.Config, serviceID, prefix string, cb func(busy bool)) {
	now := time.Now().UnixMilli()
	if c, ok := busyCache[serviceID]; ok && c.expires > now {
		cb(c.busy)
		return
	}
	fetchLoadThreshold(cfg, serviceID, prefix, func(threshold float64) {
		cfg.RedisClient.HGet(prefix+"pool", loadScoreField, func(response resp.Value) {
			busy := false
			if response.Error() == nil && !response.IsNull() {
				if score, err := strconv.ParseFloat(strings.TrimSpace(response.String()), 64); err == nil {
					busy = score >= threshold
				}
			}
			busyCache[serviceID] = busyCacheEntry{busy: busy, expires: time.Now().UnixMilli() + busyProbeTTLMS}
			cb(busy)
		})
	})
}

// fetchLoadThreshold 服务准入阈值（带 60s 缓存）。
func fetchLoadThreshold(cfg *config.Config, serviceID, prefix string, cb func(threshold float64)) {
	now := time.Now().UnixMilli()
	if c, ok := thresholdCache[serviceID]; ok && c.expires > now {
		cb(c.value)
		return
	}
	cfg.RedisClient.HGet(prefix+"conf", loadThresholdField, func(response resp.Value) {
		threshold := defaultLoadThreshold
		if response.Error() == nil && !response.IsNull() {
			if v, err := strconv.ParseFloat(strings.TrimSpace(response.String()), 64); err == nil && v > 0 {
				threshold = v
			}
		}
		thresholdCache[serviceID] = thresholdCacheEntry{value: threshold, expires: time.Now().UnixMilli() + thresholdCacheTTLMS}
		cb(threshold)
	})
}

// onPollTick tick 扫表（框架 100ms 底座）：派发到期表项的 ADMIT EVAL，
// 回收截止已过/回调失联的表项，Redis 失联时整体 fail-open。
func onPollTick() {
	cfg := activeCfg
	if cfg == nil || cfg.RedisClient == nil {
		return
	}
	if !cfg.RedisClient.Ready() {
		// Redis 失联：全部等待请求 fail-open（对齐既有口径：只注入不排队）
		if len(pendingQueue) > 0 {
			log.Warnf("%s: redis not ready, fail-open %d pending waiter(s)", pluginName, len(pendingQueue))
			drainPendingFailOpen()
		}
		return
	}

	now := time.Now().UnixMilli()
	dispatched := 0
	for key, e := range pendingQueue {
		if e.inFlight {
			if now-e.dispatchedAt > stuckDispatchTimeoutMS {
				// EVAL 回调永久失联（请求上下文已被宿主删除等）：强制回收防泄漏
				log.Warnf("%s: pending entry stuck in-flight, drop, service=%s member=%d:%s",
					pluginName, e.qc.serviceID, e.qc.tierIdx, e.qc.requestID)
				removePending(key)
			}
			continue
		}
		if now >= e.qc.deadline {
			// 截止时间已过（正常由 EVAL ttl 判定，此处覆盖表项滞留的场景）
			if err := proxywasm.SetEffectiveContext(e.ctxID); err == nil {
				e.ctx.SetContext(ctxQueueState, stateDone)
				removePending(key)
				reject(http.StatusTooManyRequests, "ttl-expired",
					"queue wait exceeded tier ttl")
			} else {
				removePending(key)
			}
			continue
		}
		if e.nextDue > now {
			continue
		}
		if dispatched >= maxDispatchPerTick {
			// 单拍派发上限：表项超限时强制节流，下一拍继续
			continue
		}
		dispatched++
		dispatchAdmitRound(key)
	}

	if len(pendingQueue) > pendingWarnLimit && !overflowWarned {
		log.Warnf("%s: pending queue overflow: %d waiter(s) exceed limit %d, dispatch throttled to %d/tick",
			pluginName, len(pendingQueue), pendingWarnLimit, maxDispatchPerTick)
		overflowWarned = true
	} else if len(pendingQueue) <= pendingWarnLimit && overflowWarned {
		overflowWarned = false
	}
}

// drainPendingFailOpen Redis 失联时清空等待表并放行全部挂起请求。
func drainPendingFailOpen() {
	for key, e := range pendingQueue {
		if err := proxywasm.SetEffectiveContext(e.ctxID); err != nil {
			// 上下文已删除：仅摘表
			removePending(key)
			continue
		}
		e.ctx.SetContext(ctxQueueState, stateDone)
		removePending(key)
		proxywasm.ResumeHttpRequest()
	}
}

// reject 429 + x-ai-drop-reason（对齐 xgs 终版拒绝语义）。
// 必须在目标请求的生效上下文中调用。
func reject(statusCode uint32, reason, message string) {
	_ = proxywasm.SendHttpResponseWithDetail(statusCode,
		pluginName+"."+reason,
		util.CreateHeaders("Content-Type", "application/json", util.DropReasonHeader, reason),
		[]byte(fmt.Sprintf(`{"error":{"message":%q,"type":%q,"code":%q}}`,
			message, pluginName, reason)), -1)
}

// onHttpRequestBody 执行链第 2 段：JSON body 顶层无条件覆盖
// "priority": ENGINE_PRIORITY[tier]（P0=-10/P1=-5/P2=0/P3=10）。
// 客户端传入的 priority 值一律不读、一律覆盖；非 JSON/无 tier 静默跳过。
func onHttpRequestBody(ctx wrapper.HttpContext, cfg config.Config, body []byte) types.Action {
	state, _ := ctx.GetContext(ctxQueueState).(queueState)
	if state == stateWaiting {
		// 理论不可达：headers 阶段未恢复不会进入 body
		return types.ActionPause
	}

	if !cfg.EnableRequestBodyInject {
		return types.ActionContinue
	}

	tier, _ := ctx.GetContext(ctxTier).(config.TierCode)
	enginePriority, ok := config.EnginePriority[tier]
	if !ok {
		// 无档位（未启用优先级的流量）：零改动直通
		return types.ActionContinue
	}

	if len(body) == 0 {
		return types.ActionContinue
	}
	method := strings.ToUpper(ctx.Method())
	if method != "POST" && method != "PUT" && method != "PATCH" {
		return types.ActionContinue
	}
	contentType := strings.ToLower(util.GetRequestHeader("content-type"))
	if !strings.Contains(contentType, "application/json") {
		return types.ActionContinue
	}

	parsed := gjson.ParseBytes(body)
	if !parsed.IsObject() {
		return types.ActionContinue
	}

	newBody, err := sjsonSetPriority(body, enginePriority)
	if err != nil {
		log.Warnf("%s: inject priority failed: %v", pluginName, err)
		return types.ActionContinue
	}
	if err := proxywasm.ReplaceHttpRequestBody(newBody); err != nil {
		log.Warnf("%s: replace request body failed: %v", pluginName, err)
		return types.ActionContinue
	}
	// body 长度可能变化，同步 Content-Length（wrapper 不自动处理）
	_ = proxywasm.ReplaceHttpRequestHeader("content-length", fmt.Sprintf("%d", len(newBody)))
	return types.ActionContinue
}

// sjsonSetPriority 用 sjson 语义写入 priority 字段（顶层无条件覆盖）。
// 为避免引入 sjson 依赖到 wasm 目标，直接用 gjson 读原始字节再拼接：
// 在顶层对象最后一个键后追加 / 覆盖 "priority"。
func sjsonSetPriority(body []byte, priority int) ([]byte, error) {
	parsed := gjson.ParseBytes(body)
	if existing := parsed.Get("priority"); existing.Exists() {
		// 覆盖已有值（无条件覆盖：客户端伪造值不可能存活）
		start, end := existing.Index, existing.Index+len(existing.Raw)
		if start <= 0 || end > len(body) {
			return nil, fmt.Errorf("priority value out of range")
		}
		out := make([]byte, 0, len(body)+8)
		out = append(out, body[:start]...)
		out = append(out, []byte(fmt.Sprintf("%d", priority))...)
		out = append(out, body[end:]...)
		return out, nil
	}
	// 顶层无 priority：在结束大括号前插入
	lastBrace := -1
	for i := len(body) - 1; i >= 0; i-- {
		if body[i] == '}' {
			lastBrace = i
			break
		}
	}
	if lastBrace <= 0 {
		return nil, fmt.Errorf("no top-level object found")
	}
	inner := strings.TrimRight(string(body[:lastBrace]), " \t\r\n")
	out := make([]byte, 0, len(body)+24)
	out = append(out, inner...)
	// 空对象（"{"）不加逗号；已有键时补分隔逗号
	if len(inner) > 0 && !strings.HasSuffix(inner, "{") {
		out = append(out, ',')
	}
	out = append(out, []byte(fmt.Sprintf(`"priority":%d`, priority))...)
	out = append(out, body[lastBrace:]...)
	return out, nil
}

// onProcessStreamDone 执行链第 3 段：摘除等待表项 + 释放在途租约（幂等
// RELEASE EVAL）。客户端断连同样触发 stream done，兜底清理必达；Redis 故障
// 只记日志，租约 TTL（脚本内夹紧 180s）兜底回收。
func onProcessStreamDone(ctx wrapper.HttpContext, cfg config.Config) {
	qc, ok := ctx.GetContext("aiPriorityQueueCtx").(*queueContext)
	if !ok || qc == nil {
		return
	}
	if key, _ := ctx.GetContext(ctxPendingKey).(string); key != "" {
		if removePending(key) {
			log.Debugf("%s: pending waiter dropped on stream done, service=%s member=%d:%s",
				pluginName, qc.serviceID, qc.tierIdx, qc.requestID)
		}
	}

	admitted, _ := ctx.GetContext("aiPriorityQueueAdmitted").(bool)
	if !admitted {
		return
	}

	args := []interface{}{qc.prefix, qc.tierIdx, qc.requestID}
	err := cfg.RedisClient.Eval(RELEASE_SCRIPT, 1,
		[]interface{}{qc.prefix + "active"}, args, func(response resp.Value) {
			if err := response.Error(); err != nil {
				log.Errorf("%s: release eval error: %v", pluginName, err)
				return
			}
			log.Debugf("%s: released lease, service=%s member=%d:%s",
				pluginName, qc.serviceID, qc.tierIdx, qc.requestID)
		})
	if err != nil {
		log.Errorf("%s: release dispatch failed: %v", pluginName, err)
	}
}
