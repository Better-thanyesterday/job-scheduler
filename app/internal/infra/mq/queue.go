package mq

import (
	"github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

// 队列/交换机常量。
//
// 当前 MQ 只承载一件事：DAG 执行结果回执（唤醒加速，DB 终态仍是唯一事实源）。
// cron 族的执行结果由 executor 直接落库、planner/reaper 轮询感知，不需要 MQ 回执；
// 旧的执行队列拓扑（Normal/Delay/Priority/DLQ）随执行通道改走 gRPC 推送而废弃，
// 不再声明——避免无生产者/无消费者的死队列在 broker 上堆积。
const (

	// 执行结果队列：dag 族按 成功 / 执行失败 / 超时 分流。
	// dag 引擎消费这三条只做"唤醒加速"，DB 终态仍是唯一事实源
	QueueDagSuccess = "job.result.dag.success"
	QueueDagFailed  = "job.result.dag.failed"
	QueueDagTimeout = "job.result.dag.timeout"

	// 结果路由键（与队列同名，都绑在 direct 交换机上）
	RKDagSuccess = QueueDagSuccess
	RKDagFailed  = QueueDagFailed
	RKDagTimeout = QueueDagTimeout
)

// RoutingKeyForResult 按"来源(dag/cron) × 结果(成功/执行失败/超时)"选结果路由键。
// failureClass 见 coordination.FailureClass* 常量，仅失败时有意义。
// cron 来源返回空串：cron 执行结果无 MQ 回执，调用方应跳过投递
func RoutingKeyForResult(fromDag, success bool, failureClass string) string {
	if !fromDag {
		return ""
	}
	if success {
		return RKDagSuccess
	}
	if failureClass == "timeout" {
		return RKDagTimeout
	}
	return RKDagFailed
}

// QueueOptions 队列声明选项
type QueueOptions struct {
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp091.Table
}

func (c *Client) DeclareQueues() error {
	ch, err := c.getMgmtChannel()
	if err != nil {
		zap.L().Info("declarequeue get mgmtchannel fail:", zap.Error(err))
		return err
	}

	// DAG 执行结果队列 ×3，同名路由键绑 direct 交换机
	resultQueues := []struct{ name, rk string }{
		{QueueDagSuccess, RKDagSuccess},
		{QueueDagFailed, RKDagFailed},
		{QueueDagTimeout, RKDagTimeout},
	}
	for _, q := range resultQueues {
		if _, err := ch.QueueDeclare(q.name, true, false, false, false, nil); err != nil {
			zap.L().Info("declare result queue failed:", zap.String("queue", q.name), zap.Error(err))
			return err
		}
		if err := ch.QueueBind(q.name, q.rk, ExchangeDirect, false, nil); err != nil {
			zap.L().Info("bind result queue failed:", zap.String("queue", q.name), zap.Error(err))
			return err
		}
	}
	return nil
}
