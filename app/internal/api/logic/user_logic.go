package logic

import (
	"context"
	"database/sql"
	"errors"
	"job-scheduler/app/internal/api/pkg/crypto"
	"job-scheduler/app/internal/api/pkg/jwt"
	"job-scheduler/app/internal/api/types"
	userdomain "job-scheduler/app/internal/domain/user"
	"job-scheduler/app/internal/infra/mysql/sqlx/model"
	repo "job-scheduler/app/internal/infra/mysql/sqlx/repo"
	snowflake "job-scheduler/app/pkg/snowflake"
	"strconv"
	"strings"

	"github.com/go-sql-driver/mysql"
	"go.uber.org/zap"
)

type UserLogic struct {
	userRepo *repo.UserRepo
}

// 对外错误分类哨兵：controller 按 errors.Is 映射业务错误码
var (
	// ErrUserOrPassword 登录失败统一口径（不区分"用户不存在/密码错误"，防枚举）
	ErrUserOrPassword = errors.New("用户名或密码错误")
	// ErrPhoneRegistered 手机号已被注册
	ErrPhoneRegistered = errors.New("手机号已注册")
	// ErrInternal 内部依赖失败（DB/签名等），对外一律"系统繁忙"，不得伪装成凭证错误
	ErrInternal = errors.New("系统繁忙，请稍后重试")
	// ErrAccountDisabled 冻结/注销账号尝试登录（不并入凭证错误，方便运营排查）
	ErrAccountDisabled = errors.New("账号已被冻结或注销")
)

func NewUserLogic(userRepo *repo.UserRepo) *UserLogic {
	return &UserLogic{
		userRepo: userRepo,
	}
}

func (u *UserLogic) Login(ctx context.Context, in *types.LoginReq) (*types.LoginResp, error) {
	var user *model.User
	user, err := u.userRepo.QueryByUserName(ctx, in.Username)
	// 2. 统一错误处理：不管是"用户不存在"还是"密码错误"，对外都返回一样的模糊错误
	if err != nil {
		if errors.Is(err, userdomain.ErrNotFound) {
			// 用户不存在 → 打 Debug 日志，与"密码错误"共用统一口径，防枚举
			zap.L().Debug("login failed: user not found", zap.String("username", in.Username))
			return nil, ErrUserOrPassword
		}
		// 真正的数据库异常 → 打 Error 日志，按内部错误处理，不冒充凭证错误
		zap.L().Error("login query db error", zap.String("username", in.Username), zap.Error(err))
		return nil, ErrInternal
	}
	if !crypto.VerifyPassword(user.PasswordHash, in.Password) {
		zap.L().Debug("login failed: password mismatch", zap.String("username", in.Username))
		return nil, ErrUserOrPassword
	}
	// 状态校验：冻结(2)/注销(3)的账号不允许登录（status 枚举 1-正常 2-冻结 3-注销）
	if user.Status != 1 {
		zap.L().Warn("login rejected: account disabled",
			zap.Int64("user_id", user.ID), zap.Int32("status", user.Status))
		return nil, ErrAccountDisabled
	}
	token, err := jwt.GenToken(user.ID, in.Username)
	if err != nil {
		zap.L().Error("gen jwt token failed", zap.Int64("user_id", user.ID), zap.Error(err))
		return nil, ErrInternal
	}
	return &types.LoginResp{
		Success: true,
		UserId:  strconv.FormatInt(user.ID, 10),
		Token:   token,
	}, nil
}

func (u *UserLogic) Register(ctx context.Context, in *types.RegisterReq) (*types.RegisterResp, error) {
	id := snowflake.GenID()
	hash, err := crypto.GeneratePassword(in.Password)
	if err != nil {
		zap.L().Error("hash password failed", zap.String("phone", in.Phone), zap.Error(err))
		return nil, ErrInternal
	}
	// username 随机生成：撞唯一键是随机事件，换一个重试即可（上限 3 次）；
	// 手机号撞 uk_users_phone 是确定冲突，立即拒绝
	var username string
	for attempt := 0; attempt < 3; attempt++ {
		username, err = crypto.GenerateUserName(strconv.FormatInt(id, 10))
		if err != nil {
			zap.L().Error("genusername failed", zap.String("phone", in.Phone), zap.Error(err))
			return nil, ErrInternal
		}
		user := &model.User{
			Phone:        sql.NullString{String: in.Phone, Valid: true},
			PasswordHash: hash,
			ID:           id,
			Username:     username,
			// 显式给枚举初值：零值 0 不在枚举内（role 3=只读用户最小权限，status 1=正常）
			Role:   3,
			Status: 1,
		}
		err = u.userRepo.Create(ctx, user)
		if err == nil {
			return &types.RegisterResp{
				Username: username,
				UserId:   id,
			}, nil
		}
		var mysqlErr *mysql.MySQLError
		if !errors.As(err, &mysqlErr) || mysqlErr.Number != 1062 {
			break // 非唯一键冲突：真正的 DB 异常，走内部错误
		}
		if strings.Contains(mysqlErr.Message, "phone") {
			return nil, ErrPhoneRegistered
		}
		// username 唯一键冲突：继续循环换名重试
		zap.L().Warn("register username conflict, retrying",
			zap.Int64("user_id", id), zap.String("username", username))
	}
	zap.L().Error("register insert db error", zap.String("phone", in.Phone), zap.Error(err))
	return nil, ErrInternal
}
