package router

import (
	"job-scheduler/app/internal/api/controller"
	middlewares "job-scheduler/app/internal/api/middleware"
	"job-scheduler/app/pkg/logger"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

func SetupRouter(mode string,
	userCtrl *controller.UserController,
	taskCtrl *controller.TaskController,
	scheduleCtrl *controller.ScheduleController,
	executionCtrl *controller.ExecutionController,
	dagCtrl *controller.DagController,
) *gin.Engine {
	if mode == gin.ReleaseMode {
		gin.SetMode(gin.ReleaseMode)
	}
	r := gin.New()
	// 全局中间件。限流默认开启：20 rps 持续、突发 100（超限 429）
	r.Use(middlewares.RateLimitMiddleware(50*time.Millisecond, 100))
	r.Use(logger.GinLogger(), logger.GinRecovery(true))
	r.Use(middlewares.CORS()) // 跨域支持，如不需要可注释

	// 公开路由：注册 / 登录
	v1 := r.Group("/api/v1")
	RegisterUserRoutes(v1, userCtrl)

	// 需要鉴权的相关路由（必须登录）
	v1Auth := r.Group("/api/v1")
	v1Auth.Use(middlewares.JWTAuthMiddleware())
	RegisterTaskRoutes(v1Auth, taskCtrl)
	RegisterScheduleRoutes(v1Auth, scheduleCtrl)
	RegisterExecutionRoutes(v1Auth, executionCtrl)
	RegisterDagRoutes(v1Auth, dagCtrl)

	// pprof 不在业务路由上暴露，改走 cmd/api/main.go 里的独立 debug 端口(127.0.0.1:6060)
	r.NoRoute(func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"msg": "404",
		})
	})

	return r
}
