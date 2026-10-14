package controller

import (
	"errors"
	"strconv"

	"job-scheduler/app/internal/api/constants"
	"job-scheduler/app/internal/api/logic"
	"job-scheduler/app/internal/api/types"
	"job-scheduler/app/internal/domain/schedule"

	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"
	"go.uber.org/zap"
)

type ScheduleController struct {
	scheduleLogic *logic.ScheduleLogic
}

func NewScheduleController(scheduleLogic *logic.ScheduleLogic) *ScheduleController {
	return &ScheduleController{
		scheduleLogic: scheduleLogic,
	}
}

// Create 创建调度：POST /schedules/create
func (s *ScheduleController) Create(c *gin.Context) {
	req := new(types.CreateScheduleReq)
	if err := c.ShouldBindJSON(req); err != nil {
		zap.L().Warn("CreateSchedule with invalid param",
			zap.Error(err),
			zap.String("path", c.Request.URL.Path),
		)
		errs, ok := err.(validator.ValidationErrors)
		if !ok {
			ResponseError(c, constants.CodeInvalidParam)
			return
		}
		ResponseErrorWithMsg(c, constants.CodeInvalidParam, removeTopStruct(errs.Translate(trans)))
		return
	}
	userId, _, err := GetCurrentUser(c)
	if err != nil {
		ResponseError(c, constants.CodeNeedLogin)
		return
	}
	resp, err := s.scheduleLogic.Create(c.Request.Context(), req, userId)
	if err != nil {
		respondScheduleError(c, "调度创建失败", 0, err)
		return
	}
	ResponseSuccess(c, resp)
}

func (s *ScheduleController) Get(c *gin.Context) {
	id, ok := parseIDParam(c, "schedule")
	if !ok {
		return
	}
	resp, err := s.scheduleLogic.Get(c.Request.Context(), id)
	if err != nil {
		respondScheduleError(c, "调度查询失败", id, err)
		return
	}
	ResponseSuccess(c, resp)
}

func (s *ScheduleController) List(c *gin.Context) {
	resp, err := s.scheduleLogic.List(c.Request.Context())
	if err != nil {
		respondScheduleError(c, "调度列表查询失败", 0, err)
		return
	}
	ResponseSuccess(c, resp)
}

func (s *ScheduleController) Pause(c *gin.Context) {
	id, ok := parseIDParam(c, "schedule")
	if !ok {
		return
	}
	if err := s.scheduleLogic.Pause(c.Request.Context(), id); err != nil {
		respondScheduleError(c, "调度暂停失败", id, err)
		return
	}
	ResponseSuccess(c, "")
}

func (s *ScheduleController) Resume(c *gin.Context) {
	id, ok := parseIDParam(c, "schedule")
	if !ok {
		return
	}
	if err := s.scheduleLogic.Resume(c.Request.Context(), id); err != nil {
		respondScheduleError(c, "调度恢复失败", id, err)
		return
	}
	ResponseSuccess(c, "")
}

func (s *ScheduleController) Delete(c *gin.Context) {
	id, ok := parseIDParam(c, "schedule")
	if !ok {
		return
	}
	if err := s.scheduleLogic.Delete(c.Request.Context(), id); err != nil {
		respondScheduleError(c, "调度删除失败", id, err)
		return
	}
	ResponseSuccess(c, "")
}

// parseIDParam 解析路径上的 :id
func parseIDParam(c *gin.Context, what string) (int64, bool) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		zap.L().Warn("invalid id param",
			zap.String("resource", what),
			zap.String("id", c.Param("id")),
			zap.String("path", c.Request.URL.Path),
		)
		ResponseError(c, constants.CodeInvalidParam)
		return 0, false
	}
	return id, true
}

// respondScheduleError 领域错误 → 业务错误码：不存在给 404 语义，
// 状态机拒绝（如暂停已暂停的调度）算非法请求，
// 乐观锁冲突（engine 推进/另一实例并发启停抢先后写）给冲突码让前端重试，其余归服务端
func respondScheduleError(c *gin.Context, action string, id int64, err error) {
	zap.L().Error(action, zap.Int64("schedule_id", id), zap.Error(err))
	switch {
	case errors.Is(err, schedule.ErrNotFound):
		ResponseError(c, constants.CodeNotFound)
	case errors.Is(err, schedule.ErrConflict):
		ResponseError(c, constants.CodeConflict)
	case errors.Is(err, schedule.ErrWrongStatus),
		errors.Is(err, schedule.ErrInvalidCron),
		errors.Is(err, schedule.ErrInvalidPolicy),
		errors.Is(err, schedule.ErrInvalidTimezone),
		errors.Is(err, schedule.ErrTaskNotFound):
		ResponseErrorWithMsg(c, constants.CodeInvalidParam, err.Error())
	default:
		ResponseError(c, constants.CodeServerBusy)
	}
}
