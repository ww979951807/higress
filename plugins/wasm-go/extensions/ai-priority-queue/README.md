---
title: AI 优先级队列
keywords: [ AI网关, 优先级队列, 推理服务 ]
description: AI 优先级队列插件配置参考
---

## 功能说明

`ai-priority-queue` 插件为 AI 推理服务提供基于服务端档位的优先级排队与准入控制。核心能力：

1. **档位防伪造**：优先级只来自服务端 Redis 映射，客户端传入的任何优先级头/body 值均不读取。
2. **准入排队**：同一推理服务的并发请求按 P0 > P1 > P2 > P3 严格优先级准入；高分档先入，同级先到先得；分档硬上限打满时排队等待。
3. **等待超时**：排队等待超过该档位的等待 TTL 直接返回 429。等待期间请求挂起（wasm 无阻塞 sleep），由插件级等待登记表 + 定时器扫表驱动续拍：卡名额（在途销号，毫秒级事件）按 `max_poll_ms` 快档轮询，卡忙闲（忙闲分由控制面按 5s 周期更新）按 `idle_poll_ms` 慢档轮询，避免百级排队者以 Redis 往返为节拍打爆 Redis。
4. **防饿死**：P1..P3 各档配置 min_rps 令牌，即使 P0 持续占满，低档位仍按最小配额放行；P0 另有独立预留百分比，保证高分档预留容量不被挤占。
5. **引擎优先级注入**：准入后把 vLLM 语义的 priority（P0=-10 / P1=-5 / P2=0 / P3=10）无条件覆盖进 JSON 请求体 `priority` 字段。
6. **fail-open**：Redis 故障、配置缺失等异常场景一律放行不拦截。

档位来源（按序回退）：

| 顺序 | 身份来源 | Redis 键 | 说明 |
| ---- | --------------- | ----------------------------- | ------------------------------ |
| 1 | `x-mse-credential` 请求头（key-auth 注入的 apikey） | `pq:tier:cred:{api_key}` HASH field `tier` | apikey 档位 |
| 2 | `x-mse-consumer` 请求头（用户 id） | `pq:tier:user:{user_id}` HASH field `tier` | 用户档位（cred 未命中时回退） |
| 3 | 均未命中 | - | 无档位，直接放行（不注入不排队） |

档位查询结果进程内缓存 60 秒（`cache_ttl` 可调），控制面改档后最长 60s 生效。

排队仅对推理服务内部路由生效：路由名匹配 `ai-route-{32位task_id}.internal` 时按 `{task_id}` 作为 service 维度排队；`{task_id}-models` 路由与租户聚合路由只注入 priority 不排队。

## 运行属性

插件执行阶段：`默认阶段`
插件执行优先级：`220`

## 配置说明

| 名称 | 数据类型 | 填写要求 | 默认值 | 描述 |
| --------------- | ------ | ---- | ------- | ---------------------------------------- |
| `redis` | object | 必填 | - | redis 相关配置 |
| `cache_ttl` | int | 选填 | 60 | 档位映射进程内缓存时长，单位秒 |
| `max_poll_ms` | int | 选填 | 100 | 等待轮询快档间隔：卡名额（在途销号）或忙闲未知时的轮询间隔，单位毫秒 |
| `idle_poll_ms` | int | 选填 | 2000 | 等待轮询慢档间隔：卡忙闲（`load_score` ≥ `load_threshold`）时的轮询间隔，单位毫秒 |
| `request_body` | bool | 选填 | true | 是否把引擎 priority 覆盖进 JSON 请求体 |

`redis` 中每一项的配置字段说明

| 配置项 | 类型 | 必填 | 默认值 | 说明 |
| ------------- | ------ | ---- | ------- | ------------------------------- |
| service_name | string | 必填 | - | redis 服务名称，带服务类型的完整 FQDN 名称，例如 redis-stack.higress-system.svc.cluster.local |
| service_port | int | 否 | 服务类型为固定地址（static service）默认值为 80，其他为 6379 | redis 服务端口 |
| username | string | 否 | - | redis 用户名 |
| password | string | 否 | - | redis 密码 |
| timeout | int | 否 | 1000 | redis 连接超时时间，单位毫秒 |
| database | int | 否 | 0 | 使用的数据库 id，例如配置为 1，对应 `SELECT 1` |

## 准入判定语义

插件内置 ADMIT / RELEASE 两段 Redis Lua（与控制面同源下发语义，对齐既有算力调度终版实现）：

- **严格优先级**：`ahead = zcard(q:{tier}) + zcard(active:{tier}) + sum(zcard(q:i) + zcard(active:i), i < tier)`，`pos <= C - ahead` 才可准入（C = replicas × concurrency_per_replica）。
- **同级 FCFS**：等待队列 score 为请求截止时间戳，先到先得。
- **分档硬上限**：本档在途 + 排队达 `max_concurrent`（`pool` 字段）返回 `full`。
- **等待 TTL**：请求入队时间超过该档位 `wait_ttl_ms`（`conf` 字段）返回 `ttl`。
- **P0 预留**：`low_cap = C - floor(C × p0_reserve_pct)`，低于 P0 的档位合计在途不超过 low_cap。
- **防饿死令牌**：P1..P3 每档 `min_rps` 令牌（`take_token`），拿到令牌的请求绕过 ahead 不等式强行放行。
- **全局限速**：可选 `max_admit_rps` 令牌桶限制整体准入速率。
- **租约 TTL**：准入即写入 `lease`（在途租约），TTL 夹紧至 180000ms，请求结束（ProcessStreamDone）由 RELEASE 脚本幂等释放。
- **等待续拍**：返回 `waiting` 时暂停请求（`HeaderStopAllIterationAndWatermark`）并登记进插件级等待表，由定时器扫表按分档节奏续拍 EVAL，直至 admitted / full / ttl。判定逻辑全部在 ADMIT 脚本内，轮询调度只决定"下一拍何时发"。
- **分档轮询**：`waiting` 后按服务忙闲预判选档——`pool.load_score` ≥ `conf.load_threshold` 视为卡忙闲，用 `idle_poll_ms`（默认 2000ms）；卡名额或忙闲未知用 `max_poll_ms`（默认 100ms）。忙闲预判结果缓存 5s（对齐控制面忙闲分更新周期），阈值缓存 60s。
- **登记表回收**：admitted / disabled / full / ttl / fail-open / 断连（stream done）均即时摘表；在途 EVAL 回调超过 30s 未返回强制回收；等待表超过 10000 条时告警并将单拍派发限流到 128 个 EVAL（派发速率上限 1280 次/秒）。

## 拒绝响应

排队被拒时返回 HTTP 429，并携带响应头 `x-ai-drop-reason`：

| 值 | 场景 |
| -------------- | ------------------------------ |
| `saturated` | 本档位并发硬上限打满 |
| `ttl-expired` | 排队等待超过该档位等待 TTL |

## 配置示例

### 单节点 redis-stack + 档位排队

```yaml
redis:
  service_name: redis-stack.higress-system.svc.cluster.local
  service_port: 6379
  timeout: 1000
  database: 0
cache_ttl: 60
max_poll_ms: 100
idle_poll_ms: 2000
request_body: true
```

### 关闭请求体注入（只做排队与 429，不改写 body）

```yaml
request_body: false
redis:
  service_name: redis-stack.higress-system.svc.cluster.local
```

## 控制面配合

- 档位下发：控制面写 `pq:tier:cred:{api_key}` / `pq:tier:user:{user_id}`（HASH field `tier`，取值 P0..P3）。
- 服务配置：控制面写 `pq:{task_id}:conf`（enabled / max_concurrent / wait_ttl_ms / min_rps / p0_reserve_pct / max_admit_rps 等），写 `pq:{task_id}:pool`（replicas / concurrency_per_replica）。
- 全部键由控制面（computing-app-server）维护，插件只读。
