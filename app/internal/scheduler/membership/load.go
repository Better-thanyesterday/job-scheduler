package membership

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// NodesPrefix 成员与负载上报的 key 前缀，完整 key 为 /scheduler/nodes/<nodeID>。
// rebalance 的 WatchNodeInfo 按 WithPrefix 监听这个前缀
const NodesPrefix = "/scheduler/nodes/"

// NodeLoad 单节点的负载快照。字段只增不删，新增必须 omitempty
type NodeLoad struct {
	NodeID               string    `json:"node_id"`
	Addr                 string    `json:"addr,omitempty"`
	CpuUsage             float64   `json:"cpu_usage"`
	MemUsage             float64   `json:"mem_usage"`
	PendingScheduleCount int64     `json:"pending_schedule_count"`
	UpdateAt             time.Time `json:"update_at"`
}

// ReportLoad 一次性上报（调试/手工用）。
// ★ 常驻进程不要用它：不带租约的 Put 会把 Register 建立的租约 key 覆盖成
// 无租约 key，节点崩溃后就永远退不出成员表。常驻路径走 LoadReporter
func ReportLoad(ctx context.Context, cli *clientv3.Client, nodeID string, load *NodeLoad) error {
	body, err := marshalLoad(nodeID, load)
	if err != nil {
		return err
	}
	if _, err := cli.Put(ctx, NodesPrefix+nodeID, body); err != nil {
		return fmt.Errorf("membership: report load: %w", err)
	}
	return nil
}

// marshalLoad 补默认值并序列化。nodeID 为空串时不回填（LoadReporter 场景，
// NodeID 已在首次注册时写死，心跳只刷新数值）
func marshalLoad(nodeID string, load *NodeLoad) (string, error) {
	if load == nil {
		return "", fmt.Errorf("membership: nil load for node %s", nodeID)
	}
	if nodeID != "" && load.NodeID == "" {
		load.NodeID = nodeID
	}
	if load.UpdateAt.IsZero() {
		load.UpdateAt = time.Now()
	}
	body, err := json.Marshal(load)
	if err != nil {
		return "", fmt.Errorf("membership: marshal load: %w", err)
	}
	return string(body), nil
}

// LoadReporter 周期采样本机负载，经 Registration 上报：
// value 挂在注册租约上，成员身份不变，只刷新心跳内容
type LoadReporter struct {
	reg      *Registration
	interval time.Duration
	// sample 采样函数由调用方提供：本进程指标、容器指标、
	// 或从 engine 读待处理量均可——本包不关心数据来源
	sample func() *NodeLoad
}

// NewLoadReporter interval<=0 时取 10s
func NewLoadReporter(reg *Registration, interval time.Duration, sample func() *NodeLoad) *LoadReporter {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	return &LoadReporter{reg: reg, interval: interval, sample: sample}
}

// Run 阻塞循环：每 interval 上报一次；单次失败只记日志（下一轮自愈）。
// ctx 取消返回 nil
func (lr *LoadReporter) Run(ctx context.Context) error {
	ticker := time.NewTicker(lr.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			body, err := marshalLoad("", lr.sample())
			if err != nil {
				zap.L().Warn("membership: 负载采样序列化失败", zap.Error(err))
				continue
			}
			if err := lr.reg.Update(ctx, body); err != nil {
				zap.L().Warn("membership: 负载上报失败", zap.Error(err))
			}
		}
	}
}
