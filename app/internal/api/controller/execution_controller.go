package controller

import (
	"errors"
	"job-scheduler/app/internal/api/constants"
	"job-scheduler/app/internal/api/logic"
	"job-scheduler/app/internal/api/types"
	"job-scheduler/app/internal/domain/execution"
	"job-scheduler/app/internal/domain/task"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"
	"go.uber.org/zap"
)

type ExecutionController struct {
	executionLogic *logic.ExecutionLogic
}

func NewExecutionController(executionLogic *logic.ExecutionLogic) *ExecutionController {
	return &ExecutionController{
		executionLogic: executionLogic,
	}
}

// ListByTask 执行历史分页：GET /executions?task_id=&page=
func (e *ExecutionController) ListByTask(c *gin.Context) {
	req := new(types.ListExecutionsReq)
	if err := c.ShouldBindQuery(req); err != nil {
		zap.L().Warn("ListExecutions with invalid param",
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
	resp, err := e.executionLogic.ListByTask(c.Request.Context(), req)
	if err != nil {
		respondExecutionError(c, "执行历史查询失败", req.TaskID, err)
		return
	}
	ResponseSuccess(c, resp)
}

func (e *ExecutionController) Get(c *gin.Context) {
	id, ok := parseIDParam(c, "execution")
	if !ok {
		return
	}
	resp, err := e.executionLogic.Get(c.Request.Context(), id)
	if err != nil {
		respondExecutionError(c, "执行详情查询失败", id, err)
		return
	}
	ResponseSuccess(c, resp)
}

// Trigger 手动触发一次执行：POST /tasks/:id/trigger
func (e *ExecutionController) Trigger(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		zap.L().Warn("Trigger with invalid id param", zap.Error(err),
			zap.String("path", c.Request.URL.Path))
		ResponseError(c, constants.CodeInvalidParam)
		return
	}
	userId, _, err := GetCurrentUser(c)
	if err != nil {
		ResponseError(c, constants.CodeNeedLogin)
		return
	}
	resp, err := e.executionLogic.Trigger(c.Request.Context(), id, userId)
	if err != nil {
		zap.L().Error("手动触发执行失败", zap.Int64("task_id", id),
			zap.Int64("user_id", userId), zap.Error(err))
		switch {
		case errors.Is(err, task.ErrNotFound):
			ResponseError(c, constants.CodeNotFound)
		case errors.Is(err, logic.ErrInvalidTaskInput):
			ResponseErrorWithMsg(c, constants.CodeInvalidParam, err.Error())
		default:
			respondExecutionError(c, "手动触发执行失败", id, err)
		}
		return
	}
	ResponseSuccess(c, resp)
}

func respondExecutionError(c *gin.Context, action string, id int64, err error) {
	zap.L().Error(action, zap.Int64("execution_id", id), zap.Error(err))
	switch {
	case errors.Is(err, execution.ErrNotFound):
		ResponseError(c, constants.CodeNotFound)
	case errors.Is(err, execution.ErrWrongStatus):
		ResponseError(c, constants.CodeInvalidTransition)
	case errors.Is(err, execution.ErrAlreadyClaimed):
		// 认领冲突：执行已被其他 worker 领取
		ResponseError(c, constants.CodeConflict)
	case errors.Is(err, execution.ErrRetryExhausted):
		ResponseError(c, constants.CodeInvalidTransition)
	default:
		ResponseError(c, constants.CodeServerBusy)
	}
}

// Result 溢出结果可达性：GET /executions/:id/result
// 内联结果直接返回原文；溢出信封 302 到 15 分钟预签名下载 URL——
// 大载荷直连对象存储不过 api
func (e *ExecutionController) Result(c *gin.Context) {
	id, ok := parseIDParam(c, "execution")
	if !ok {
		return
	}
	dl, err := e.executionLogic.DownloadResult(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, execution.ErrNotFound) {
			ResponseError(c, constants.CodeNotFound)
			return
		}
		zap.L().Error("获取执行结果下载失败", zap.Int64("execution_id", id), zap.Error(err))
		ResponseErrorWithMsg(c, constants.CodeServerBusy, err.Error())
		return
	}
	if dl.Redirect != "" {
		c.Redirect(http.StatusFound, dl.Redirect)
		return
	}
	ct := dl.ContentType
	if ct == "" {
		ct = "application/json"
	}
	c.Data(http.StatusOK, ct, dl.Inline)
}
