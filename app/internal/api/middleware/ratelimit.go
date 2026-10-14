package middlewares

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/juju/ratelimit"
)

// RateLimitMiddleware 全局令牌桶限流：fillInterval 滴入一个令牌，
// cap 为桶容量（突发余量）。超限返回 429 + JSON
func RateLimitMiddleware(fillInterval time.Duration, cap int64) func(c *gin.Context) {
	bucket := ratelimit.NewBucket(fillInterval, cap)
	return func(c *gin.Context) {
		if bucket.TakeAvailable(1) == 0 {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"code": http.StatusTooManyRequests,
				"msg":  "请求过于频繁，请稍后重试",
			})
			return
		}
		c.Next()
	}
}
