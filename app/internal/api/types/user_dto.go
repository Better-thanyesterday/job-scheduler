package types

// 用户域：注册 / 登录的请求与响应

type LoginReq struct {
	Username string `json:"username" binding:"required,min=6,max=12"` // 用户名
	Password string `json:"password" binding:"required,min=6,max=20"` // 密码
}

type LoginResp struct {
	Token   string `json:"token"`
	UserId  string `json:"user_id"`
	Success bool   `json:"success"`
}

type RegisterReq struct {
	Phone    string `json:"phone" binding:"required,len=11"`
	Password string `json:"password" binding:"required,min=6,max=20"` // 密码
}

type RegisterResp struct {
	Username string `json:"username"` // 用户名（后端生成，登录时使用）
	UserId   int64  `json:"user_id"`  // 用户主键id
}
