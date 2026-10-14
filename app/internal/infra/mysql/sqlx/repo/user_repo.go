package sqlx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	userdomain "job-scheduler/app/internal/domain/user"
	"job-scheduler/app/internal/infra/mysql"
	"job-scheduler/app/internal/infra/mysql/sqlx/model"

	"github.com/jmoiron/sqlx"
)

type UserRepo struct {
	db *sqlx.DB
}

func NewUserRepo(systemDB mysql.SystemDB) *UserRepo {
	return &UserRepo{db: systemDB.DB}
}

// QueryByUserName 登录用：只取 password_hash 与 id（与 gorm 版 Select 一致，
// 其余列不查）。users 走 gorm 软删除，等价条件 deleted_at IS NULL
func (u *UserRepo) QueryByUserName(ctx context.Context, username string) (*model.User, error) {
	var po model.User
	err := u.db.GetContext(ctx, &po,
		`SELECT id, password_hash, status FROM users WHERE username = ? AND deleted_at IS NULL LIMIT 1`,
		username)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, fmt.Errorf("%w: username=%q", userdomain.ErrNotFound, username)
	case err != nil:
		return nil, fmt.Errorf("query user %q: %w", username, err)
	}
	return &po, nil
}

// Create 插入用户。email 有唯一键：空值必须落 NULL 而不是空串，
// 否则第二个未填邮箱的用户就会撞 uk_users_email（直接不写该列，交给列默认 NULL）
func (u *UserRepo) Create(ctx context.Context, user *model.User) error {
	cols := []string{"id", "username", "password_hash", "nickname", "phone", "role", "status"}
	args := []interface{}{user.ID, user.Username, user.PasswordHash, user.Nickname, user.Phone, user.Role, user.Status}
	if user.Email.String != "" { // Valid=false 时 String 为空串，同样省略该列
		cols = append(cols, "email")
		args = append(args, user.Email)
	}

	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(cols)), ",")
	_, err := u.db.ExecContext(ctx,
		`INSERT INTO users (`+strings.Join(cols, ",")+`) VALUES (`+placeholders+`)`,
		args...)
	if err != nil {
		return err
	}
	return nil
}
