package router

// 用户域：注册 / 登录（公开路由，不走 JWT 中间件）

import (
	"job-scheduler/app/internal/api/controller"

	"github.com/gin-gonic/gin"
)

func RegisterUserRoutes(v1 *gin.RouterGroup, userCtrl *controller.UserController) {
	v1.POST("/register", userCtrl.Register)
	v1.POST("/login", userCtrl.Login)
}
