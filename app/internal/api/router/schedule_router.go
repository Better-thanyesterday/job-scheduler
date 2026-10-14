package router

import (
	"job-scheduler/app/internal/api/controller"

	"github.com/gin-gonic/gin"
)

func RegisterScheduleRoutes(authGroup *gin.RouterGroup, scheduleCtrl *controller.ScheduleController) {
	schedules := authGroup.Group("/schedules")
	{
		schedules.POST("/create", scheduleCtrl.Create)
		schedules.GET("/list", scheduleCtrl.List)
		schedules.GET("/:id", scheduleCtrl.Get)
		schedules.POST("/:id/pause", scheduleCtrl.Pause)
		schedules.POST("/:id/resume", scheduleCtrl.Resume)
		schedules.DELETE("/:id", scheduleCtrl.Delete)
	}
}
