//go:build wireinject
// +build wireinject

package main

// Wire 注入声明（编译期依赖注入）。
// 本文件只给 wire 工具看，不参与正常编译；改完依赖关系后重新生成：
//
//	cd app/cmd/api && wire
//
// 生成产物 wire_gen.go 参与编译，main 只调 InitializeApp。

import (
	"github.com/gin-gonic/gin"
	"github.com/google/wire"

	"job-scheduler/app/internal/api/controller"
	"job-scheduler/app/internal/api/logic"
	"job-scheduler/app/internal/api/router"
	"job-scheduler/app/internal/domain/coordination"
	"job-scheduler/app/internal/executor/resultstore"
	infraetcd "job-scheduler/app/internal/infra/etcd"
	"job-scheduler/app/internal/infra/mysql"
	repo "job-scheduler/app/internal/infra/mysql/sqlx/repo"
	"job-scheduler/app/internal/scheduler"
)

// InitializeApp 装配整棵依赖树：
// 配置 → MySQL 连接池 + etcd 客户端 → repo/pusher → logic → controller → gin 路由。
// 返回 cleanup 时 wire 会把各 provider 的清理函数 LIFO 聚合（关闭连接池与 etcd 连接）
func InitializeApp() (*gin.Engine, func(), error) {
	wire.Build(
		// 配置
		mysql.ProvideAppConfig,
		mysql.ProvideMode,
		mysql.ProvideMySqls,
		// 连接池：ProvideDBPools 建集合，FieldsOf 拆成分域类型供 repo 构造函数直取
		mysql.ProvideDBPools,
		resultstore.ProvideStore,
		wire.FieldsOf(new(mysql.DBPools), "System", "Task", "Schedule", "Execution", "Dag"),
		// etcd 客户端 + 执行消息推送器（手动触发经 gRPC 直推 executor）
		infraetcd.ProvideConfig,
		infraetcd.ProvideClient,
		scheduler.NewPusher,
		wire.Bind(new(coordination.TaskPusher), new(*scheduler.Pusher)),
		// repo：收分域类型，构造函数即 provider
		repo.NewUserRepo,
		repo.NewTaskRepo,
		repo.NewScheduleRepo,
		repo.NewTaskSourceRepo,
		repo.NewExecutionRegistryRepo,
		repo.NewExecutionQueryRepo,
		repo.NewDagRepo,
		repo.NewDagExecutionRepo,
		// logic / controller / router：构造函数签名即 provider，直接引用
		logic.NewUserLogic,
		logic.NewTaskLogic,
		logic.NewScheduleLogic,
		logic.NewExecutionLogic,
		logic.NewDagLogic,
		controller.NewUserController,
		controller.NewTaskController,
		controller.NewScheduleController,
		controller.NewExecutionController,
		controller.NewDagController,
		router.SetupRouter,
	)
	return nil, nil, nil // wire 重写此函数体，占位仅为满足编译
}
