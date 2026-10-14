package mysql

import (
	"fmt"
	"time"

	"job-scheduler/app/settings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"
)

// 分域连接池：类型即约束——repo 构造函数只收自己域的类型，
// 拿错池在编译期报错，而不是运行时 "table doesn't exist"。
// 嵌入 *sqlx.DB，Get/Select/NamedExec 等方法全部提升，用起来和裸池无差
type (
	SystemDB    struct{ *sqlx.DB }
	TaskDB      struct{ *sqlx.DB }
	ScheduleDB  struct{ *sqlx.DB }
	ExecutionDB struct{ *sqlx.DB }
	DagDB       struct{ *sqlx.DB }
)

// opened 本进程建过的全部连接池，CloseAll 统一关闭。包内私有，不对外暴露
var opened []*sqlx.DB

// Init 建一个域专属的 sqlx 连接池（job_task/job_schedule/... 每库各调一次）。
// 只返回句柄不持有——连接池生命周期归调用方管
func Init(cfg *settings.MySQLConfig) *sqlx.DB {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		cfg.User,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		cfg.DB,
	)
	db, err := sqlx.Open("mysql", dsn) // 返回 *sqlx.DB
	if err != nil {
		zap.L().Error("connect Mysql failed", zap.String("dbname", cfg.DB), zap.Error(err))
		panic(err)
	}

	// 连接池配置（和原生完全一样，因为 *sqlx.DB 内嵌了 *sql.DB）
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(time.Hour)

	if err := db.Ping(); err != nil {
		zap.L().Error("ping Mysql failed", zap.String("dbname", cfg.DB), zap.Error(err))
		panic(err)
	}
	opened = append(opened, db)
	return db
}

// required 缺域即 panic：分库拓扑由类型固定，缺一段配置撑不起整套类型
func required(cfg map[string]*settings.MySQLConfig, name string) *settings.MySQLConfig {
	m, ok := cfg[name]
	if !ok || m == nil {
		panic(fmt.Sprintf("mysql pool %q missing: check mysqls section in configs/config.yaml", name))
	}
	return m
}

// InitAll 按固定分库拓扑建齐五个域的连接池（system/task/schedule/execution/dag）
func InitAll(cfg map[string]*settings.MySQLConfig) (
	system SystemDB, task TaskDB, schedule ScheduleDB, execution ExecutionDB, dag DagDB,
) {
	system = SystemDB{Init(required(cfg, "system"))}
	task = TaskDB{Init(required(cfg, "task"))}
	schedule = ScheduleDB{Init(required(cfg, "schedule"))}
	execution = ExecutionDB{Init(required(cfg, "execution"))}
	dag = DagDB{Init(required(cfg, "dag"))}
	return system, task, schedule, execution, dag
}

func CloseAll() {
	for _, db := range opened {
		if err := db.Close(); err != nil {
			zap.L().Error("close db failed", zap.Error(err))
		}
	}
	opened = nil
}
