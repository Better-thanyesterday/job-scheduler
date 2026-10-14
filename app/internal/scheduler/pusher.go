package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"job-scheduler/app/internal/domain/coordination"
	executor "job-scheduler/app/internal/executor/rpc"
	infraetcd "job-scheduler/app/internal/infra/etcd"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var _ coordination.TaskPusher = (*Pusher)(nil)

// Pusher 执行消息的 gRPC 推送实现：
// 从 etcd 成员表发现在线 executor（/executors/<workerID>，租约存活的都在），
// router 按"etcd 负载 + 本进程在途 + 失败熔断"择优，逐台降级重试。
// 至少一次语义：单实例失败自动换下一台，全部失败返回 ErrPushFailed；
// 消费端 ClaimPending 原子抢票保证幂等。
//
// 注：原 RabbitMQ 路由的 Priority 分流随队列拓扑一起消失，消息里的
// priority 字段保留（executor 侧将来可做内部优先级调度）
type Pusher struct {
	cli   *clientv3.Client
	route *router

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn // addr → 复用连接；推送失败即摘除
}

func NewPusher(cli *clientv3.Client) *Pusher {
	return &Pusher{
		cli: cli,
		route: newRouter(func(ctx context.Context) ([]infraetcd.ExecutorInfo, error) {
			return infraetcd.ListExecutors(ctx, cli)
		}),
		conns: make(map[string]*grpc.ClientConn),
	}
}

func (p *Pusher) Push(ctx context.Context, msg *coordination.ExecutionMessage) error {
	if err := msg.Validate(); err != nil {
		return fmt.Errorf("validate: %w (exec=%d task=%d type=%d timeout=%d cfg_len=%d)",
			err, msg.ExecID, msg.TaskID, msg.TaskType, msg.TimeoutSec, len(msg.Config))
	}
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal execution message: %w", err)
	}

	cands, err := p.route.candidates(ctx, msg.ExecID)
	if err != nil {
		return fmt.Errorf("%w: pick executor: %v", coordination.ErrPushFailed, err)
	}

	// RPC 预算：执行本身在 executor 侧脱离 RPC 生命周期（认领后即使断开也跑完），
	// 这里的超时只需覆盖"排队 + 正常执行回包"，按执行超时的 2 倍 + DB 宽限给足
	rpcTimeout := time.Duration(msg.TimeoutSec)*time.Second*2 + 30*time.Second
	cctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	req := &executor.ExecuteRequest{Payload: body}
	var lastErr error
	for _, ex := range cands {
		p.route.acquire(ex.Addr)
		resp, err := p.call(cctx, ex.Addr, req)
		p.route.release(ex.Addr, err == nil)
		if err != nil {
			lastErr = err
			zap.L().Warn("推送 executor 失败，尝试下一台",
				zap.String("addr", ex.Addr), zap.Int64("execution_id", msg.ExecID), zap.Error(err))
			p.dropConn(ex.Addr) // 连接可能已坏，摘除逼下次重拨
			continue
		}
		// 送达即成功：执行成败走 DB 终态 + planner 重试，不在推送层重试
		switch executor.ErrorCode(resp.GetErrorCode()) {
		case executor.ErrorCode_CLAIMED_FALSE:
			zap.L().Info("推送送达（重复投递已幂等丢弃）", zap.Int64("execution_id", msg.ExecID))
		case executor.ErrorCode_EXEC_FAILED:
			zap.L().Warn("推送送达，任务执行失败（重试由 planner 接管）",
				zap.Int64("execution_id", msg.ExecID), zap.String("error", resp.GetErrorMsg()))
		}
		return nil
	}
	return fmt.Errorf("%w: all %d executors failed: %v", coordination.ErrPushFailed, len(cands), lastErr)
}

// call 单次 RPC；连接按 addr 缓存复用
func (p *Pusher) call(ctx context.Context, addr string, req *executor.ExecuteRequest) (*executor.ExecuteResponse, error) {
	conn, err := p.connFor(addr)
	if err != nil {
		return nil, err
	}
	return executor.NewExecutorServiceClient(conn).Execute(ctx, req)
}

func (p *Pusher) connFor(addr string) (*grpc.ClientConn, error) {
	p.mu.Lock()
	conn, ok := p.conns[addr]
	p.mu.Unlock()
	if ok {
		return conn, nil
	}
	c, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("grpc new client %s: %w", addr, err)
	}
	p.mu.Lock()
	p.conns[addr] = c
	p.mu.Unlock()
	return c, nil
}

func (p *Pusher) dropConn(addr string) {
	p.mu.Lock()
	conn, ok := p.conns[addr]
	delete(p.conns, addr)
	p.mu.Unlock()
	if ok {
		_ = conn.Close()
	}
}
