package model

import (
	"database/sql"
	"time"
)

// User 对应 job_system.users。email/nickname/phone/last_login_at 可空
type User struct {
	ID           int64          `db:"id" json:"id"`
	Username     string         `db:"username" json:"username"`
	Email        sql.NullString `db:"email" json:"email"`
	PasswordHash string         `db:"password_hash" json:"password_hash"`
	Nickname     sql.NullString `db:"nickname" json:"nickname"`
	Phone        sql.NullString `db:"phone" json:"phone"`
	Role         int32          `db:"role" json:"role"`
	Status       int32          `db:"status" json:"status"`
	LastLoginAt  sql.NullTime   `db:"last_login_at" json:"last_login_at"`
	CreatedAt    time.Time      `db:"created_at" json:"created_at"`
	UpdatedAt    time.Time      `db:"updated_at" json:"updated_at"`
	DeletedAt    sql.NullTime   `db:"deleted_at" json:"deleted_at"`
}
