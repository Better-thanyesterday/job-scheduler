package router

import (
	"job-scheduler/app/internal/api/controller"

	"github.com/gin-gonic/gin"
)

func RegisterTaskRoutes(authGroup *gin.RouterGroup, taskCtrl *controller.TaskController) {
	tasks := authGroup.Group("/tasks")
	{
		tasks.POST("/create", taskCtrl.Create)
		tasks.GET("/list", taskCtrl.List)
		tasks.GET("/:id", taskCtrl.Get)
		tasks.PUT("/:id", taskCtrl.Update)
		tasks.DELETE("/:id", taskCtrl.Delete)
	}
}
