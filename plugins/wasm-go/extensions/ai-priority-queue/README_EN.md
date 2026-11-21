---
title: AI Priority Queue
keywords: [ AI Gateway, Priority Queue, Inference Service ]
description: AI priority queue plugin configuration reference
---

## Function Description

The `ai-priority-queue` plugin provides server-side tier based priority queuing and admission control for AI inference services. Core capabilities:

1. **Anti-forgery tiers**: priorities come only from the server-side Redis mapping; any priority header/body value sent by the client is never read.
2. **Admission queuing**: concurrent requests to the same inference service are admitted under strict priority P0 > P1 > P2 > P3; higher tiers first, FCFS within a tier; requests wait in queue once the tier hard limit is reached.
3. **Wait timeout**: requests waiting longer than the tier wait TTL are rejected with 429. While waiting the request is suspended (wasm has no blocking sleep) and re-polling is driven by a plugin-level pending registry plus a timer tick: blocked on quota (in-flight slot release, a millisecond-level event) polls at the fast interval `max_poll_ms`; blocked on load score (updated by the control plane every 5s) polls at the slow interval `idle_poll_ms`, so hundreds of waiters do not hammer Redis at Redis-round-trip cadence.
4. **Anti-starvation**: tiers P1..P3 each have a min_rps token quota, so lower tiers still get admitted at their minimum rate even when P0 saturates; P0 additionally has a dedicated reserve percentage so its capacity cannot be squeezed.
5. **Engine priority injection**: after admission the vLLM-style priority (P0=-10 / P1=-5 / P2=0 / P3=10) is unconditionally overwritten into the JSON request body `priority` field.
6. **Fail-open**: on Redis failures or missing configuration the request is always allowed through.

Tier sources (fallback order):

| Order | Identity source | Redis key | Description |
| ----- | ------------------------ | ------------------------------------ | ---------------------------------------------------- |
| 1 | `x-mse-credential` header (apikey injected by key-auth) | `pq:tier:cred:{api_key}` HASH field `tier` | API key tier |
| 2 | `x-mse-consumer` header (user id) | `pq:tier:user:{user_id}` HASH field `tier` | User tier (fallback when cred misses) |
| 3 | Neither present | - | No tier, pass through directly (no inject, no queue) |

Tier lookups are cached in-process for 60 seconds (tunable via `cache_ttl`); tier changes take effect after at most 60s plus propagation delay.

Queuing only applies to inference-service internal routes: when the route name matches `ai-route-{32-hex task_id}.internal`, `{task_id}` is used as the service dimension; `{task_id}-models` routes and tenant aggregate routes only inject priority without queuing.

## Runtime Properties

Plugin execution phase: `default phase`
Plugin execution priority: `220`

## Configuration Description

| Name | Data Type | Required | Default | Description |
| --------------- | ------ | ---- | ------- | ---------------------------------------- |
| `redis` | object | Yes | - | Redis related configuration |
| `cache_ttl` | int | Optional | 60 | In-process TTL of the tier mapping cache, in seconds |
| `max_poll_ms` | int | Optional | 100 | Fast poll interval: used when blocked on quota (slot release) or when load is unknown, in milliseconds |
| `idle_poll_ms` | int | Optional | 2000 | Slow poll interval: used when blocked on load score (`load_score` ≥ `load_threshold`), in milliseconds |
| `request_body` | bool | Optional | true | Whether to overwrite the engine priority into the JSON request body |

Fields of `redis`

| Field | Type | Required | Default | Description |
| ------------- | ------ | ---- | ------- | ------------------------------- |
| service_name | string | Yes | - | Full FQDN of the redis service, e.g. redis-stack.higress-system.svc.cluster.local |
| service_port | int | No | 80 for static services, 6379 otherwise | Redis service port |
| username | string | No | - | Redis username |
| password | string | No | - | Redis password |
| timeout | int | No | 1000 | Redis connection timeout, in milliseconds |
| database | int | No | 0 | Database id, e.g. 1 means `SELECT 1` |

## Admission Semantics

The plugin embeds two Redis Lua scripts (ADMIT / RELEASE) aligned with the existing compute-scheduling final implementation:

- **Strict priority**: `ahead = zcard(q:{tier}) + zcard(active:{tier}) + sum(zcard(q:i) + zcard(active:i), i < tier)`; admit only when `pos <= C - ahead` where `C = replicas × concurrency_per_replica`.
- **FCFS within a tier**: queue score is the request deadline timestamp, first come first served.
- **Per-tier hard limit**: returns `full` when the tier's in-flight + queued reaches `max_concurrent` (`pool` field).
- **Wait TTL**: returns `ttl` when the request has been queued longer than the tier's `wait_ttl_ms` (`conf` field).
- **P0 reserve**: `low_cap = C - floor(C × p0_reserve_pct)`; total in-flight of tiers below P0 must not exceed low_cap.
- **Anti-starvation tokens**: each of P1..P3 has a `min_rps` token quota (`take_token`); a request holding a token bypasses the ahead inequality and is admitted.
- **Global rate limit**: optional `max_admit_rps` token bucket limiting the overall admission rate.
- **Lease TTL**: admission writes an in-flight `lease` whose TTL is clamped to 180000ms; RELEASE removes it idempotently on request completion (ProcessStreamDone).
- **Wait polling**: on `waiting` the request is paused (`HeaderStopAllIterationAndWatermark`) and registered into the plugin-level pending registry; a timer tick re-triggers EVAL at the tiered cadence until admitted / full / ttl. All admission logic stays inside the ADMIT script; the polling scheduler only decides when the next attempt fires.
- **Tiered polling**: after `waiting`, a per-service load probe picks the interval — `pool.load_score` ≥ `conf.load_threshold` means blocked on load, using `idle_poll_ms` (default 2000ms); blocked on quota or unknown load uses `max_poll_ms` (default 100ms). The probe result is cached for 5s (aligned with the control plane score update period), the threshold for 60s.
- **Registry cleanup**: admitted / disabled / full / ttl / fail-open / client disconnect (stream done) all remove the entry immediately; an in-flight EVAL whose callback never returns is force-reclaimed after 30s; when the registry exceeds 10000 entries the plugin logs a warning and throttles dispatches to 128 EVALs per tick (up to 1280 dispatches per second).

## Rejection Response

Rejected requests receive HTTP 429 with the `x-ai-drop-reason` header:

| Value | Scenario |
| -------------- | ------------------------------ |
| `saturated` | The tier's concurrency hard limit is full |
| `ttl-expired` | The request waited longer than the tier wait TTL |

## Configuration Example

### Single-node redis-stack with tier queuing

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

### Disable body injection (queuing and 429 only, body untouched)

```yaml
request_body: false
redis:
  service_name: redis-stack.higress-system.svc.cluster.local
```

## Control Plane Integration

- Tier publishing: the control plane writes `pq:tier:cred:{api_key}` / `pq:tier:user:{user_id}` (HASH field `tier`, values P0..P3).
- Service configuration: the control plane writes `pq:{task_id}:conf` (enabled / max_concurrent / wait_ttl_ms / min_rps / p0_reserve_pct / max_admit_rps) and `pq:{task_id}:pool` (replicas / concurrency_per_replica).
- All keys are maintained by the control plane (computing-app-server); the plugin is read-only.
