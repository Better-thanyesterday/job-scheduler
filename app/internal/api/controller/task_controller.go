package controller

import (
	"context"
	"errors"
	"job-scheduler/app/internal/api/constants"
	"job-scheduler/app/internal/api/logic"
	"job-scheduler/app/internal/api/types"
	"job-scheduler/app/internal/domain/task"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"
	"go.uber.org/zap"
)

type TaskController struct {
	taskLogic *logic.TaskLogic
}

func NewTaskController(taskLogic *logic.TaskLogic) *TaskController {
	return &TaskController{
		taskLogic: taskLogic,
	}
}

// respondTaskError 领域错误 → 业务错误码：重名/不存在/状态机拒绝各有其码，
// 领域校验失败（config、重试策略非法）把详情透给用户，其余（DB 等）归服务繁忙
func respondTaskError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, task.ErrNameDuplicated):
		ResponseError(c, constants.CodeTaskNameDuplicated)
	case errors.Is(err, task.ErrNotFound):
		ResponseError(c, constants.CodeNotFound)
	case errors.Is(err, task.ErrInvalidTransition),
		errors.Is(err, task.ErrAlreadyDeleted):
		ResponseError(c, constants.CodeInvalidTransition)
	case errors.Is(err, logic.ErrInvalidTaskInput):
		ResponseErrorWithMsg(c, constants.CodeInvalidParam, err.Error())
	default:
		ResponseError(c, constants.CodeServerBusy)
	}
}

func (t *TaskController) Create(c *gin.Context) {
	req := new(types.CreateTaskReq)
	//参数绑定和参数校验
	if err := c.ShouldBind(req); err != nil {
		zap.L().Warn("CreateTask with invalid param",
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
	//参数转化(可选)
	resp, err := t.taskLogic.Create(c.Request.Context(), req, userId)
	if err != nil {
		zap.L().Error("创建任务失败", zap.Error(err),
			zap.Int64("user_id", userId),
			zap.String("task_name", req.Name),
		)
		respondTaskError(c, err)
		return
	}
	//包装返回值
	ResponseSuccess(c, resp)
}

func (t *TaskController) List(c *gin.Context) {
	req := new(types.ListTasksReq)
	if err := c.ShouldBind(req); err != nil {
		zap.L().Warn("ListQuery with invalid param",
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
	resp, err := t.taskLogic.List(c.Request.Context(), req, userId)
	if err != nil {
		// 客户端主动断开（如压测收尾）导致 ctx 取消：不是服务端故障，降级处理不打 ERROR
		if errors.Is(err, context.Canceled) {
			return
		}
		zap.L().Error("批量查询失败", zap.Error(err),
			zap.Int64("user_id", userId),
		)
		respondTaskError(c, err)
		return
	}
	ResponseSuccess(c, resp)
}

func (t *TaskController) Update(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		zap.L().Warn("Task Update with invalid id param",
			zap.Error(err),
			zap.String("path", c.Request.URL.Path),
		)
		ResponseError(c, constants.CodeInvalidParam)
		return
	}
	req := new(types.UpdateTaskReq)
	if err := c.ShouldBind(req); err != nil {
		zap.L().Warn("Task Update with invalid param",
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
	resp, err := t.taskLogic.Update(c.Request.Context(), id, req)
	if err != nil {
		zap.L().Error("更新失败", zap.Error(err),
			zap.Int64("user_id", userId),
		)
		respondTaskError(c, err)
		return
	}
	ResponseSuccess(c, resp)
}

func (t *TaskController) Delete(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		zap.L().Warn("Task Delete with invalid id param",
			zap.Error(err),
			zap.String("path", c.Request.URL.Path),
		)
		ResponseError(c, constants.CodeInvalidParam)
		return
	}
	userId, _, err := GetCurrentUser(c)
	if err != nil {
		ResponseError(c, constants.CodeNeedLogin)
		return
	}
	err = t.taskLogic.Delete(c.Request.Context(), id)
	if err != nil {
		zap.L().Error("任务删除失败", zap.Error(err),
			zap.Int64("user_id", userId),
		)
		respondTaskError(c, err)
		return
	}
	ResponseSuccess(c, "")
}

func (t *TaskController) Get(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		zap.L().Warn("Task Get with invalid id param",
			zap.Error(err),
			zap.String("path", c.Request.URL.Path),
		)
		ResponseError(c, constants.CodeInvalidParam)
		return
	}
	userId, _, err := GetCurrentUser(c)
	if err != nil {
		ResponseError(c, constants.CodeNeedLogin)
		return
	}
	resp, err := t.taskLogic.Get(c.Request.Context(), id)
	if err != nil {
		zap.L().Error("任务查询失败", zap.Error(err),
			zap.Int64("user_id", userId),
		)
		respondTaskError(c, err)
		return
	}
	ResponseSuccess(c, resp)
}
