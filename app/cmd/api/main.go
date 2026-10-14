package main

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"job-scheduler/app/internal/api/controller"
	"job-scheduler/app/internal/api/pkg/jwt"
	"job-scheduler/app/internal/infra/mysql"
	"job-scheduler/app/pkg/logger"
	"job-scheduler/app/pkg/metrics"
	"job-scheduler/app/pkg/snowflake"
	"job-scheduler/app/settings"
	"log"
	"net/http"
	// pprof handler 注册到 http.DefaultServeMux，由下方独立 debug 端口 serve，
	// 不进业务路由（8888），生产环境不对外
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	// 1. 加载配置
	if err := settings.Init(); err != nil {
		fmt.Printf("init settings failed, err:%v\n", err)
		return
	}
	fmt.Println(settings.Conf.Load())
	fmt.Println(settings.Conf.Load().LogConfig == nil)

	// 2. 初始化日志
	if err := logger.Init(settings.Conf.Load().LogConfig, settings.Conf.Load().Mode, "api"); err != nil {
		fmt.Printf("init logger failed, err:%v\n", err)
		return
	}
	defer zap.L().Sync()
	zap.L().Debug("logger init success...")

	// 2.1 JWT 签发配置（auth 段；不配置走包内开发默认值）
	if auth := settings.Conf.Load().AuthConfig; auth != nil {
		jwt.Configure(auth.JwtSecret, auth.JwtExpire)
	}

	// 2.2 /metrics 指标端口：独立 mux，只读，进程内计数器/直方图全在这暴露
	metricsPort := settings.Conf.Load().MetricsPort
	if metricsPort == 0 {
		metricsPort = 9101 // 与 scheduler 的 9100 区分
	}
	go func() {
		metricsSrv := &http.Server{Addr: fmt.Sprintf("127.0.0.1:%d", metricsPort), Handler: metrics.Handler()}
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			zap.L().Error("api metrics server exited", zap.Error(err))
		}
	}()

	// 3. 初始化 MySQL 数据库（InitAll 内部连接失败会 panic，无需 err 分支）
	mysql.InitAll(settings.Conf.Load().MySqls)
	defer mysql.CloseAll()

	// 3.1 依赖注入（wire 编译期生成，见 wire.go / wire_gen.go；
	//     改依赖关系后重新生成：cd app/cmd/api && wire）
	r, cleanup, err := InitializeApp()
	if err != nil {
		zap.L().Fatal("initialize app failed", zap.Error(err))
	}
	defer cleanup()

	// 加载雪花算法配置
	if err := snowflake.Init(settings.Conf.Load().StartTime, settings.Conf.Load().MachineID); err != nil {
		fmt.Printf("init snowflask failed,err:%#v\n", err)
	}

	// 初始化gin框架内置的校验器使用的翻译器
	if err := controller.InitTrans("zh"); err != nil {
		fmt.Printf("init validator trans failed,err:%#v\n", err)
	}

	// 5. 启动服务（优雅关机）。路由树已由 InitializeApp 装配完成
	fmt.Println(settings.Conf.Load().Port)
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", settings.Conf.Load().Port),
		Handler: r,
	}

	go func() {
		// 开启一个goroutine启动服务
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()

	// pprof 调试端口（方案 A）：独立于业务端口、只绑本机。
	//   CPU 30s 窗口: go tool pprof http://127.0.0.1:6060/debug/pprof/profile?seconds=30
	//   堆:           go tool pprof http://127.0.0.1:6060/debug/pprof/heap
	//   goroutine:    go tool pprof http://127.0.0.1:6060/debug/pprof/goroutine
	go func() {
		if err := http.ListenAndServe("127.0.0.1:6060", nil); err != nil {
			zap.L().Error("pprof debug server exited", zap.Error(err))
		}
	}()

	// 等待中断信号来优雅地关闭服务器，为关闭服务器操作设置一个5秒的超时
	quit := make(chan os.Signal, 1) // 创建一个接收信号的通道
	// kill 默认会发送 syscall.SIGTERM 信号
	// kill -2 发送 syscall.SIGINT 信号，我们常用的Ctrl+C就是触发系统SIGINT信号
	// kill -9 发送 syscall.SIGKILL 信号，但是不能被捕获，所以不需要添加它
	// signal.Notify把收到的 syscall.SIGINT或syscall.SIGTERM 信号转发给quit
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM) // 此处不会阻塞
	<-quit                                               // 阻塞在此，当接收到上述两种信号时才会往下执行
	zap.L().Info("Shutdown Server ...")
	// 创建一个5秒超时的context
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// 5秒内优雅关闭服务（将未处理完的请求处理完再关闭服务），超过5秒就超时退出
	if err := srv.Shutdown(ctx); err != nil {
		zap.L().Fatal("Server Shutdown", zap.Error(err))
	}

	zap.L().Info("Server exiting")
}
