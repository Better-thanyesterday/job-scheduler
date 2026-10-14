package controller

import (
	"errors"
	"job-scheduler/app/internal/api/constants"
	"job-scheduler/app/internal/api/logic"
	"job-scheduler/app/internal/api/types"

	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"
	"go.uber.org/zap"
)

type UserController struct {
	userLogic *logic.UserLogic
}

func NewUserController(userLogic *logic.UserLogic) *UserController {
	return &UserController{
		userLogic: userLogic,
	}
}

// respondUserError 按 logic 层哨兵错误映射业务码：凭证/重复注册如实反馈，
// DB、签名等内部失败一律"服务繁忙"，不再误报成"用户名或密码错误"
func respondUserError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, logic.ErrUserOrPassword):
		ResponseError(c, constants.CodeInvalidPassword)
	case errors.Is(err, logic.ErrPhoneRegistered):
		ResponseErrorWithMsg(c, constants.CodeUserExist, err.Error())
	default:
		ResponseError(c, constants.CodeServerBusy)
	}
}

func (u *UserController) Login(c *gin.Context) {
	req := new(types.LoginReq)
	if err := c.ShouldBind(req); err != nil {
		zap.L().Info("Login with invalid param", zap.Error(err))
		errs, ok := err.(validator.ValidationErrors)
		if !ok {
			ResponseError(c, constants.CodeInvalidParam)
			return
		}
		ResponseErrorWithMsg(c, constants.CodeInvalidParam, removeTopStruct(errs.Translate(trans)))
		return
	}
	resp, err := u.userLogic.Login(c, req)
	if err != nil {
		respondUserError(c, err)
		return
	}
	ResponseSuccess(c, resp)
}

func (u *UserController) Register(c *gin.Context) {
	req := new(types.RegisterReq)
	if err := c.ShouldBindJSON(req); err != nil {
		zap.L().Error("Login with invalid param", zap.Error(err))
		errs, ok := err.(validator.ValidationErrors)
		if !ok {
			ResponseError(c, constants.CodeInvalidParam)
			return
		}
		ResponseErrorWithMsg(c, constants.CodeInvalidParam, removeTopStruct(errs.Translate(trans)))
		return
	}
	resp, err := u.userLogic.Register(c.Request.Context(), req)
	if err != nil {
		respondUserError(c, err)
		return
	}
	ResponseSuccess(c, resp)
}
