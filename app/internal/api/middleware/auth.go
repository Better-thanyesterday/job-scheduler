package middlewares

import (
	"context"
	"job-scheduler/app/internal/api/constants"
	"job-scheduler/app/internal/api/controller"
	"job-scheduler/app/internal/api/pkg/jwt"
	"strings"

	"github.com/gin-gonic/gin"
)

// JWTAuthMiddleware 基于JWT的认证中间件
func JWTAuthMiddleware() func(c *gin.Context) {
	return func(c *gin.Context) {
		// 客户端携带Token有三种方式 1.放在请求头 2.放在请求体 3.放在URI
		// 这里Token放在Header的Authorization中，并使用Bearer开头
		//Authorization: Bearer xxxxxx.xxxx.xxx
		authHeader := c.Request.Header.Get("Authorization")
		if authHeader == "" {
			controller.ResponseError(c, constants.CodeNeedLogin)
			c.Abort()
			return
		}
		// 按空格分割
		parts := strings.SplitN(authHeader, " ", 2)
		if !(len(parts) == 2 && parts[0] == "Bearer") {
			controller.ResponseError(c, constants.CodeInvalidToken)
			c.Abort()
			return
		}
		// parts[1]是获取到的tokenString，我们使用之前定义好的解析JWT的函数来解析它
		mc, err := jwt.ParseToken(parts[1])
		if err != nil {
			controller.ResponseError(c, constants.CodeInvalidToken)
			c.Abort()
			return
		}
		//  注入到标准的 context.Context 中，供 Logic / Repo 层使用
		ctx := context.WithValue(c.Request.Context(), controller.CtxUserIDKey, mc.UserID)
		ctx = context.WithValue(ctx, controller.CtxUserName, mc.Username)
		c.Request = c.Request.WithContext(ctx)
		// 将当前请求的userid信息保存到请求的上下文c上
		c.Set(controller.CtxUserIDKey, mc.UserID)
		c.Set(controller.CtxUserName, mc.Username)
		c.Next() // 后续的处理请求的函数中 可以用过c.Get("ctxUserIDKey")来获取当前请求的用户信息
	}
}
