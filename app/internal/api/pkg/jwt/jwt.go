package jwt

import (
	"errors"
	"time"

	"github.com/golang-jwt/jwt/v4"
)

// CustomClaims 登录态声明：在标准 claims 上追加业务字段
type CustomClaims struct {
	UserID   int64  `json:"user_id"`
	Username string `json:"username"`
	jwt.RegisteredClaims
}

// 签名密钥与有效期：默认值仅供本地开发；
// 生产从 configs 的 auth 段注入（cmd/api 启动时调 Configure），泄露即可伪造任意 token
var (
	customSecret        = []byte("afgshjstthts*9")
	tokenExpireDuration = 24 * 3 * time.Hour
)

// Configure 注入签名密钥与有效期（settings 的 auth 段），api 启动时调用一次
func Configure(secret string, expireHours int) {
	if secret != "" {
		customSecret = []byte(secret)
	}
	if expireHours > 0 {
		tokenExpireDuration = time.Duration(expireHours) * time.Hour
	}
}

// GenToken 生成JWT
func GenToken(userID int64, username string) (string, error) {
	claims := CustomClaims{
		userID,
		username,
		jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(tokenExpireDuration)),
			Issuer:    "api",
		},
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(customSecret)
}

// ParseToken 解析JWT
func ParseToken(tokenString string) (*CustomClaims, error) {
	var cc = new(CustomClaims)
	token, err := jwt.ParseWithClaims(tokenString, cc, func(token *jwt.Token) (i interface{}, err error) {
		return customSecret, nil
	})
	if err != nil {
		return nil, err
	}
	if token.Valid {
		return cc, nil
	}
	return nil, errors.New("invalid token")
}
