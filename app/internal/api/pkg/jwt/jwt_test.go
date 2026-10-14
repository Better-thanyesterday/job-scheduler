package jwt

import (
	"testing"
	"time"
)

// Configure 注入的密钥/有效期必须真实影响签发与解析——
// 守护"auth.jwt_secret 尚未生效"的回归（曾硬编码导致改配置无效）
func TestConfigureAffectsSigning(t *testing.T) {
	old := string(customSecret)
	defer Configure(old, 72) // 还原，避免污染同包其他用例

	Configure("unit-test-secret", 1)
	token, err := GenToken(7, "alice")
	if err != nil {
		t.Fatalf("GenToken: %v", err)
	}
	// 正确密钥能解
	claims, err := ParseToken(token)
	if err != nil || claims.UserID != 7 || claims.Username != "alice" {
		t.Fatalf("ParseToken with configured secret: %v %+v", err, claims)
	}
	// 旧密钥必须解不开（证明签名确实用了新密钥）
	Configure("another-secret", 1)
	defer Configure("unit-test-secret", 1)
	if _, err := ParseToken(token); err == nil {
		t.Fatal("换密钥后旧 token 必须失效")
	}
	// 有效期生效：1 小时过期的 token exp 在 2 小时内
	Configure("unit-test-secret", 1)
	token, _ = GenToken(8, "bob")
	claims, err = ParseToken(token)
	if err != nil {
		t.Fatalf("ParseToken: %v", err)
	}
	if until := time.Until(claims.ExpiresAt.Time); until <= 0 || until > 2*time.Hour {
		t.Fatalf("jwt_expire 未生效: token 有效期 %v", until)
	}
}
