// Package executorv1 ExecutorService 的 gRPC 服务端实现。
//
// 生成的 executor.pb.go / executor_grpc.pb.go 由 rpc/executor.proto 生成，
//
//	重新生成：protoc --go_out=. --go_opt=module=job-scheduler \
//	  --go-grpc_out=. --go-grpc_opt=module=job-scheduler \
//	  app/internal/executor/rpc/executor.proto
package rpc

import (
	"context"
	"encoding/json"
	"time"

	"job-scheduler/app/internal/domain/coordination"
	"job-scheduler/app/internal/executor/handler"
	"job-scheduler/app/pkg/metrics"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// dbGrace 终态落库在执行超时之后的宽限：执行 ctx 按 timeout_sec 掐断后，
// FinishFailed/UpdateTerminal 这些 DB 操作还要一点时间
const dbGrace = 30 * time.Second

// Server ExecutorService 服务端。薄壳：解析 payload → 委托 handler → 映射响应
type Server struct {
	UnimplementedExecutorServiceServer
	handler *handler.Handler
}

func NewServer(h *handler.Handler) *Server {
	return &Server{handler: h}
}

// Execute 处理一次推送。错误码映射：
//
//	InvalidArgument → payload 非法，重试无意义
//	Unavailable     → executor 侧基础设施抖动，调用方换实例重投
//	OK              → 送达，结果看 ExecuteResponse 字段
func (s *Server) Execute(ctx context.Context, req *ExecuteRequest) (*ExecuteResponse, error) {
	var msg coordination.ExecutionMessage
	if err := json.Unmarshal(req.GetPayload(), &msg); err != nil {
		metrics.IncGrpcExecution("invalid")
		return nil, status.Errorf(codes.InvalidArgument, "unmarshal execution message: %v", err)
	}
	if err := msg.Validate(); err != nil {
		metrics.IncGrpcExecution("invalid")
		return nil, status.Errorf(codes.InvalidArgument, "invalid execution message: %v", err)
	}

	// ★ 执行脱离 RPC 生命周期：一旦认领，即使调用方断开/超时，任务也要
	// 跑完写终态（否则行卡 Running 等 reaper 收割）。故用独立 Background
	// ctx，预算 = 执行超时 + DB 宽限；handler 内部还会按 timeout_sec 再掐一层
	execCtx, cancel := context.WithTimeout(context.Background(),
		time.Duration(msg.TimeoutSec)*time.Second+dbGrace)
	defer cancel()

	outcome, err := s.handler.Handle(execCtx, &msg)
	if err != nil {
		metrics.IncGrpcExecution("unavailable")
		zap.L().Warn("executor: 处理推送失败（基础设施抖动）",
			zap.Int64("execution_id", msg.ExecID), zap.Error(err))
		return nil, status.Errorf(codes.Unavailable, "handle execution %d: %v", msg.ExecID, err)
	}

	metrics.IncGrpcExecution("ok")
	resp := &ExecuteResponse{
		Claimed: outcome.Claimed,
		Success: outcome.Success,
	}
	if !outcome.Success {
		resp.ErrorMsg = outcome.ErrorMsg
		if outcome.Claimed {
			resp.ErrorCode = int32(ErrorCode_EXEC_FAILED)
			zap.L().Warn("executor: 任务执行失败",
				zap.Int64("execution_id", msg.ExecID), zap.String("error", outcome.ErrorMsg))
		} else {
			resp.ErrorCode = int32(ErrorCode_CLAIMED_FALSE)
			zap.L().Info("executor: 重复投递已幂等丢弃", zap.Int64("execution_id", msg.ExecID))
		}
	} else {
		resp.ErrorCode = int32(ErrorCode_OK)
	}
	return resp, nil
}
