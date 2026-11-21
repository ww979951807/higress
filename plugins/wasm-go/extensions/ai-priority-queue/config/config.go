package config

import (
	"errors"
	"strings"

	"github.com/higress-group/wasm-go/pkg/wrapper"
	"github.com/tidwall/gjson"
)

// DefaultCacheTTLSeconds 档位映射（cred/consumer → tier）的进程内缓存时长（秒）。
// 控制面改档位后最长 60s + 下发延迟生效，属方案已确认的可接受延迟。
const DefaultCacheTTLSeconds = 60

// DefaultMaxPollMilliseconds 快速轮询档间隔（毫秒）：卡名额（active 销号是
// 毫秒级事件）或忙闲未知时使用。wasm 无阻塞 sleep，等待用"登记表 + tick
// 扫表"驱动（wrapper.RegisterTickFunc 100ms 底座），本值即表项的快档 nextDue。
const DefaultMaxPollMilliseconds = 100

// DefaultIdlePollMilliseconds 慢速轮询档间隔（毫秒）：卡忙闲（load_score ≥
// load_threshold）时使用。忙闲分由控制面按 5s 周期更新，快档轮询全是重复读，
// 故默认放宽到 2000ms。
const DefaultIdlePollMilliseconds = 2000

// TierCode / TierIndex / EnginePriority 四级优先级的三个视图。
// tier 只来自服务端 Redis 映射，客户端传入的任何优先级头/body 值均不读取。
type TierCode string

const (
	TierP0 TierCode = "P0"
	TierP1 TierCode = "P1"
	TierP2 TierCode = "P2"
	TierP3 TierCode = "P3"
)

// EnginePriority vLLM 语义：数值越小越优先。
var EnginePriority = map[TierCode]int{
	TierP0: -10,
	TierP1: -5,
	TierP2: 0,
	TierP3: 10,
}

// TierIndex 等待/在途分档队列序号（w:0..3 / active:0..3）。
var TierIndex = map[TierCode]int{
	TierP0: 0,
	TierP1: 1,
	TierP2: 2,
	TierP3: 3,
}

// Config 插件配置（随插件实例 rawConfigurations 下发）。
type Config struct {
	// Redis 单节点连接（与 master 现网 redis-stack 同实例，控制面同源下发）
	ServiceName string
	ServicePort int
	Username    string
	Password    string
	Timeout     int
	Database    int
	RedisClient wrapper.RedisClient

	// TierCacheTTLSeconds 档位缓存时长（秒）
	TierCacheTTLSeconds int
	// MaxPollMilliseconds 快速轮询档间隔（毫秒）：卡名额/忙闲未知时的轮询间隔
	MaxPollMilliseconds int
	// IdlePollMilliseconds 慢速轮询档间隔（毫秒）：卡忙闲（load_score ≥
	// load_threshold）时的轮询间隔
	IdlePollMilliseconds int
	// EnableRequestBodyInject 是否把引擎 priority 覆盖进 JSON 请求体
	EnableRequestBodyInject bool
}

// InitRedisClient 解析 redis 配置段并初始化单节点客户端。
// master 现网为单节点 redis-stack（无 cluster），沿用 FQDNCluster 形态的
// wrapper.RedisClusterClient 即可按单地址访问。
func InitRedisClient(json gjson.Result, config *Config) error {
	redisConfig := json.Get("redis")
	if !redisConfig.Exists() {
		return errors.New("missing redis in config")
	}

	serviceName := redisConfig.Get("service_name").String()
	if serviceName == "" {
		return errors.New("redis service name must not be empty")
	}

	servicePort := int(redisConfig.Get("service_port").Int())
	if servicePort == 0 {
		if strings.HasSuffix(serviceName, ".static") {
			// use default logic port which is 80 for static service
			servicePort = 80
		} else {
			servicePort = 6379
		}
	}

	username := redisConfig.Get("username").String()
	password := redisConfig.Get("password").String()
	timeout := int(redisConfig.Get("timeout").Int())
	if timeout == 0 {
		timeout = 1000
	}
	database := int(redisConfig.Get("database").Int())

	config.ServiceName = serviceName
	config.ServicePort = servicePort
	config.Username = username
	config.Password = password
	config.Timeout = timeout
	config.Database = database
	config.RedisClient = wrapper.NewRedisClusterClient(wrapper.FQDNCluster{
		FQDN: serviceName,
		Port: int64(servicePort),
	})
	return config.RedisClient.Init(username, password, int64(timeout), wrapper.WithDataBase(database))
}

// ParseConfig 解析插件配置；Redis 为必填（无 Redis 时插件完全零行为，按
// fail-open 口径等同"未启用"，但配置缺失属配置错误，插件启动即报错暴露）。
func ParseConfig(json gjson.Result, config *Config) error {
	if err := InitRedisClient(json, config); err != nil {
		return err
	}
	return ParseOptions(json, config)
}

// ParseOptions 解析除 Redis 连接外的全部可选项（纯解析，不发起 host 调用，
// 单测可直接覆盖）。非法/非正值逐项回默认。
func ParseOptions(json gjson.Result, config *Config) error {
	// 档位缓存时长（秒），非法/非正值回默认 60
	config.TierCacheTTLSeconds = DefaultCacheTTLSeconds
	if v := json.Get("cache_ttl"); v.Exists() {
		if ttl := int(v.Int()); ttl > 0 {
			config.TierCacheTTLSeconds = ttl
		}
	}

	// 等待轮询快档间隔（毫秒）：卡名额/忙闲未知时使用，非法/非正值回默认 100
	config.MaxPollMilliseconds = DefaultMaxPollMilliseconds
	if v := json.Get("max_poll_ms"); v.Exists() {
		if ms := int(v.Int()); ms > 0 {
			config.MaxPollMilliseconds = ms
		}
	}

	// 等待轮询慢档间隔（毫秒）：卡忙闲时使用，非法/非正值回默认 2000
	config.IdlePollMilliseconds = DefaultIdlePollMilliseconds
	if v := json.Get("idle_poll_ms"); v.Exists() {
		if ms := int(v.Int()); ms > 0 {
			config.IdlePollMilliseconds = ms
		}
	}

	// body 注入开关，默认开启
	config.EnableRequestBodyInject = true
	if v := json.Get("request_body"); v.Exists() {
		config.EnableRequestBodyInject = v.Bool()
	}

	return nil
}

// TierFromCode 把 Redis 中存储的档位字符串归一化为 TierCode。
func TierFromCode(code string) (TierCode, bool) {
	switch TierCode(strings.ToUpper(strings.TrimSpace(code))) {
	case TierP0:
		return TierP0, true
	case TierP1:
		return TierP1, true
	case TierP2:
		return TierP2, true
	case TierP3:
		return TierP3, true
	}
	return "", false
}
