package router

import (
	"job-scheduler/app/internal/api/controller"

	"github.com/gin-gonic/gin"
)

func RegisterDagRoutes(authGroup *gin.RouterGroup, dagCtrl *controller.DagController) {
	dags := authGroup.Group("/dags")
	{
		dags.POST("/import", dagCtrl.Import) // 请求体为 YAML 文本
		dags.GET("/:id", dagCtrl.Get)
		dags.POST("/:id/run", dagCtrl.Run)         // 发起一次运行，请求体为可选 JSON 上下文
		dags.PUT("/:id", dagCtrl.Update)           // 整体替换定义，请求体同 Import
		dags.POST("/:id/enable", dagCtrl.Enable)   // 启用
		dags.POST("/:id/disable", dagCtrl.Disable) // 停用（不再接受新运行）
		dags.DELETE("/:id", dagCtrl.Delete)        // 软删
	}
}
