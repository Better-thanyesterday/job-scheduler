package controller

import (
	"context"
	"encoding/json"
	"errors"
	"job-scheduler/app/internal/api/constants"
	"job-scheduler/app/internal/api/logic"
	"job-scheduler/app/internal/api/types"
	"job-scheduler/app/internal/domain/dag"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"go.uber.org/zap"
)

type DagController struct {
	dagLogic *logic.DagLogic
}

func NewDagController(dagLogic *logic.DagLogic) *DagController {
	return &DagController{dagLogic: dagLogic}
}

// respondDagError 领域错误 → 业务错误码：YAML/校验类把详情透给用户，
// 未找到单独给码，其余归服务繁忙
func respondDagError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, logic.ErrInvalidDagInput):
		ResponseErrorWithMsg(c, constants.CodeInvalidParam, err.Error())
	case errors.Is(err, dag.ErrNotFound):
		ResponseError(c, constants.CodeNotFound)
	case errors.Is(err, dag.ErrWrongStatus):
		ResponseError(c, constants.CodeInvalidTransition)
	default:
		ResponseError(c, constants.CodeServerBusy)
	}
}

// Run 发起一次 DAG 运行：POST /dags/:id/run，请求体为可选 JSON（运行上下文快照）
func (d *DagController) Run(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		ResponseError(c, constants.CodeInvalidParam)
		return
	}
	if _, _, err := GetCurrentUser(c); err != nil {
		ResponseError(c, constants.CodeNeedLogin)
		return
	}
	// 请求体可选：空体 = 无上下文快照
	var runCtx json.RawMessage
	if c.Request.Body != nil && c.Request.ContentLength > 0 {
		if err := c.ShouldBindJSON(&runCtx); err != nil {
			ResponseErrorWithMsg(c, constants.CodeInvalidParam, "context 不是合法 JSON: "+err.Error())
			return
		}
	}
	resp, err := d.dagLogic.Run(c.Request.Context(), id, runCtx)
	if err != nil {
		zap.L().Error("发起 DAG 运行失败", zap.Int64("dag_id", id), zap.Error(err))
		respondDagError(c, err)
		return
	}
	ResponseSuccess(c, resp)
}

// Update 整体替换定义：PUT /dags/:id，请求体同 Import 的 YAML
func (d *DagController) Update(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		ResponseError(c, constants.CodeInvalidParam)
		return
	}
	req := new(types.DagYAML)
	if err := c.ShouldBindWith(req, binding.YAML); err != nil {
		ResponseErrorWithMsg(c, constants.CodeInvalidParam, "YAML 解析失败: "+err.Error())
		return
	}
	if _, _, err := GetCurrentUser(c); err != nil {
		ResponseError(c, constants.CodeNeedLogin)
		return
	}
	resp, err := d.dagLogic.Update(c.Request.Context(), id, req)
	if err != nil {
		zap.L().Error("更新 DAG 失败", zap.Int64("dag_id", id), zap.Error(err))
		respondDagError(c, err)
		return
	}
	ResponseSuccess(c, resp)
}

// Enable / Disable / Delete 定义生命周期：POST /dags/:id/enable|disable，DELETE /dags/:id
func (d *DagController) Enable(c *gin.Context)  { d.changeStatus(c, (*logic.DagLogic).Enable) }
func (d *DagController) Disable(c *gin.Context) { d.changeStatus(c, (*logic.DagLogic).Disable) }
func (d *DagController) Delete(c *gin.Context)  { d.changeStatus(c, (*logic.DagLogic).Delete) }

func (d *DagController) changeStatus(c *gin.Context,
	apply func(*logic.DagLogic, context.Context, int64) (*types.DagResp, error)) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		ResponseError(c, constants.CodeInvalidParam)
		return
	}
	if _, _, err := GetCurrentUser(c); err != nil {
		ResponseError(c, constants.CodeNeedLogin)
		return
	}
	resp, err := apply(d.dagLogic, c.Request.Context(), id)
	if err != nil {
		zap.L().Error("DAG 生命周期操作失败", zap.Int64("dag_id", id), zap.Error(err))
		respondDagError(c, err)
		return
	}
	ResponseSuccess(c, resp)
}

// Import 导入 YAML 定义的 DAG。请求体整体就是 YAML（不是 JSON 包一层）
func (d *DagController) Import(c *gin.Context) {
	req := new(types.DagYAML)
	// 显式指定 YAML binding：请求体整篇 YAML，与 Content-Type 无关
	if err := c.ShouldBindWith(req, binding.YAML); err != nil {
		zap.L().Warn("ImportDag with invalid yaml",
			zap.Error(err),
			zap.String("path", c.Request.URL.Path),
		)
		ResponseErrorWithMsg(c, constants.CodeInvalidParam, "YAML 解析失败: "+err.Error())
		return
	}
	userId, _, err := GetCurrentUser(c)
	if err != nil {
		ResponseError(c, constants.CodeNeedLogin)
		return
	}
	resp, err := d.dagLogic.Import(c.Request.Context(), req, userId)
	if err != nil {
		zap.L().Error("导入 DAG 失败", zap.Error(err),
			zap.Int64("user_id", userId),
			zap.String("dag_name", req.Name),
		)
		respondDagError(c, err)
		return
	}
	ResponseSuccess(c, resp)
}

func (d *DagController) Get(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		zap.L().Warn("Dag Get with invalid id param",
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
	resp, err := d.dagLogic.Get(c.Request.Context(), id)
	if err != nil {
		zap.L().Error("DAG 查询失败", zap.Error(err),
			zap.Int64("user_id", userId),
			zap.Int64("dag_id", id),
		)
		respondDagError(c, err)
		return
	}
	ResponseSuccess(c, resp)
}
