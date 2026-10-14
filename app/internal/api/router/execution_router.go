package router

import (
	"job-scheduler/app/internal/api/controller"

	"github.com/gin-gonic/gin"
)

func RegisterExecutionRoutes(authGroup *gin.RouterGroup, executionCtrl *controller.ExecutionController) {
	executions := authGroup.Group("/executions")
	{
		executions.GET("/list", executionCtrl.ListByTask)
		executions.GET("/:id", executionCtrl.Get)
		executions.GET("/:id/result", executionCtrl.Result)
	}
	// 手动触发：在任务资源下创建一次执行（执行域的写入口）
	tasks := authGroup.Group("/tasks")
	{
		tasks.POST("/:id/trigger", executionCtrl.Trigger)
	}
}
