package mysql

// wire 的 provider 集合：只做「类型 → 实例」的装配，不含业务逻辑。
// 注入声明见 wire.go（//go:build wireinject），生成代码见 wire_gen.go。
// 重新生成：cd app/cmd/api && wire
//
// 连接池是分域类型（mysql.SystemDB/TaskDB/...），repo 构造函数只收自己域的类型，
// 错池在编译期报错——所以这里不再需要 map → repo 的映射样板

import (
	"job-scheduler/app/settings"
)

// ProvideAppConfig 返回配置快照。前置条件：main 已调 settings.Init()
func ProvideAppConfig() *settings.AppConfig { return settings.Conf.Load() }

func ProvideMode(cfg *settings.AppConfig) string { return cfg.Mode }

func ProvideMySqls(cfg *settings.AppConfig) map[string]*settings.MySQLConfig { return cfg.MySqls }

// DBPools 五个业务域连接池的集合。wire 通过 FieldsOf 拆成五个独立提供值，
// repo 构造函数按各自域类型直取
type DBPools struct {
	System    SystemDB
	Task      TaskDB
	Schedule  ScheduleDB
	Execution ExecutionDB
	Dag       DagDB
}

// ProvideDBPools 建齐全部连接池（缺域/连不上时 InitAll 内部 panic）。
// cleanup 交给 wire 聚合，main 退出路径上关闭全部池
func ProvideDBPools(mySqls map[string]*settings.MySQLConfig) (DBPools, func()) {
	system, task, schedule, execution, dag := InitAll(mySqls)
	return DBPools{
		System:    system,
		Task:      task,
		Schedule:  schedule,
		Execution: execution,
		Dag:       dag,
	}, CloseAll
}
