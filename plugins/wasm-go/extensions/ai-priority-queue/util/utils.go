package util

import (
	"regexp"
	"strings"

	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm"
)

// CredentialHeader key-auth 注入的 apikey 逻辑值（染色的第一优先来源）。
const CredentialHeader = "x-mse-credential"

// ConsumerHeader key-auth 注入的用户 id（档位回退来源）。
const ConsumerHeader = "x-mse-consumer"

// DropReasonHeader 拒绝响应的原因头，对齐 xgs 终版 429 语义。
const DropReasonHeader = "x-ai-drop-reason"

// aiRoutePattern 平台服务级 AiRoute 的内部路由名：ai-route-{task_id}.internal，
// task_id 为 32 位十六进制。`{task_id}-models`（免认证 /v1/models）与
// 租户聚合路由（{租户前缀}-{model}）不含 task_id，不匹配。
var aiRoutePattern = regexp.MustCompile(`^ai-route-([0-9a-fA-F]{32})\.internal$`)

// ServiceIDFromRouteName 从 Envoy 路由名解析 service 维度（32 位 task_id）。
// 无 service 维度（models 路由/聚合路由/普通路由）返回空串：调用方只做
// body 注入，不排队。
func ServiceIDFromRouteName(routeName string) string {
	m := aiRoutePattern.FindStringSubmatch(strings.TrimSpace(routeName))
	if len(m) < 2 {
		return ""
	}
	return strings.ToLower(m[1])
}

// GetRouteName 读取 Envoy 路由名属性；读取失败返回空串。
func GetRouteName() string {
	raw, err := proxywasm.GetProperty([]string{"route_name"})
	if err != nil {
		return ""
	}
	return string(raw)
}

// GetRequestHeader 读取请求头，缺失/出错返回空串。
func GetRequestHeader(key string) string {
	value, err := proxywasm.GetHttpRequestHeader(key)
	if err != nil {
		return ""
	}
	return value
}

// CreateHeaders k/v 序列转 [][2]string，奇数个丢弃最后一个，避免越界。
func CreateHeaders(kvs ...string) [][2]string {
	if len(kvs)%2 != 0 {
		kvs = kvs[:len(kvs)-1]
	}
	headers := make([][2]string, 0, len(kvs)/2)
	for i := 0; i < len(kvs); i += 2 {
		headers = append(headers, [2]string{kvs[i], kvs[i+1]})
	}
	return headers
}
