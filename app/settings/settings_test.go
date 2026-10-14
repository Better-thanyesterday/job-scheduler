package settings

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
)

// 守护 auth 段解码：内嵌指针结构（*AuthConfig mapstructure:"auth"）必须
// 真正填上值——它没填上时 jwt 密钥外置就是空转
func TestAuthConfigDecode(t *testing.T) {
	v := viper.New()
	v.SetConfigType("yaml")
	yaml := `
auth:
  jwt_secret: test-secret-abc
  jwt_expire: 100
mq:
  host: "127.0.0.1"
`
	if err := v.ReadConfig(strings.NewReader(yaml)); err != nil {
		t.Fatal(err)
	}
	var cfg AppConfig
	if err := v.Unmarshal(&cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.AuthConfig == nil {
		t.Fatal("AuthConfig 未解码")
	}
	if cfg.AuthConfig.JwtSecret != "test-secret-abc" || cfg.AuthConfig.JwtExpire != 100 {
		t.Fatalf("auth 段解码不符: %+v", cfg.AuthConfig)
	}
}
