package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestServiceIDFromRouteName(t *testing.T) {
	tests := []struct {
		name      string
		routeName string
		want      string
	}{
		{
			name:      "platform ai-route internal",
			routeName: "ai-route-9a342114ba8a11ecb1bf00163e1250b5.internal",
			want:      "9a342114ba8a11ecb1bf00163e1250b5",
		},
		{
			name:      "uppercase hex normalized to lower",
			routeName: "ai-route-9A342114BA8A11ECB1BF00163E1250B5.internal",
			want:      "9a342114ba8a11ecb1bf00163e1250b5",
		},
		{
			name:      "models route without task id",
			routeName: "9a342114ba8a11ecb1bf00163e1250b5-models",
			want:      "",
		},
		{
			name:      "tenant aggregate route",
			routeName: "tenant1-qwen.internal",
			want:      "",
		},
		{
			name:      "plain route",
			routeName: "test-route-default",
			want:      "",
		},
		{
			name:      "wrong hex length",
			routeName: "ai-route-9a342114.internal",
			want:      "",
		},
		{
			name:      "empty route name",
			routeName: "",
			want:      "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ServiceIDFromRouteName(tt.routeName))
		})
	}
}

func TestCreateHeaders(t *testing.T) {
	// 偶数个：正常转换
	headers := CreateHeaders("a", "1", "b", "2")
	assert.Equal(t, [][2]string{{"a", "1"}, {"b", "2"}}, headers)
	// 奇数个：丢弃最后一个，不越界
	headers = CreateHeaders("a", "1", "b")
	assert.Equal(t, [][2]string{{"a", "1"}}, headers)
	// 空
	assert.Empty(t, CreateHeaders())
}
