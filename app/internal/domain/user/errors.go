package user

import "errors"

// ErrNotFound 用户不存在（repo 按用户名/ID 查询未命中）。
// api 侧拿到它后按防枚举口径归一成"用户名或密码错误"，不外透真实原因
var ErrNotFound = errors.New("user: not found")
