package constants

type ResCode int64

const (
	CodeSuccess         ResCode = 1000 + iota // 成功
	CodeInvalidParam                          // 请求参数错误
	CodeUserExist                             // 用户已存在
	CodeUserNotExist                          // 用户不存在
	CodeInvalidPassword                       // 用户名或密码错误
	CodeServerBusy                            // 服务繁忙

	CodeInvalidToken // 无效的token
	CodeNeedLogin    // 需要登录
	CodeNoPermission // 无权限
	CodeNotFound     // 资源不存在

	CodeNotCaptcha    // 验证码错误
	CodeCaptchaExpire // 验证码过期

	CodeEmail // 激活邮件发送失败

	CodeRepeated // 禁止重复相同操作

	CodeTaskNameDuplicated // 任务名已存在
	CodeInvalidTransition  // 当前状态不允许该操作
	CodeConflict           // 操作冲突：资源已被并发修改，请刷新后重试
)

var codeMsgMap = map[ResCode]string{
	CodeSuccess:         "success",
	CodeInvalidParam:    "请求参数错误",
	CodeUserExist:       "用户已存在",
	CodeUserNotExist:    "用户不存在",
	CodeInvalidPassword: "用户名或密码错误",
	CodeServerBusy:      "服务繁忙",

	CodeInvalidToken:  "无效的token",
	CodeNeedLogin:     "需要登录",
	CodeNoPermission:  "无权限操作",
	CodeNotFound:      "资源不存在",
	CodeNotCaptcha:    "验证码错误",
	CodeCaptchaExpire: "验证码过期",

	CodeEmail: "激活邮件发送失败",

	CodeRepeated:           "禁止重复相同操作",
	CodeTaskNameDuplicated: "任务名已存在",
	CodeInvalidTransition:  "当前状态不允许该操作",
	CodeConflict:           "操作冲突，资源已被并发修改，请刷新后重试",
}

func (c ResCode) Msg() string {
	msg, ok := codeMsgMap[c]
	if !ok {
		msg = codeMsgMap[CodeServerBusy]
	}
	return msg
}
