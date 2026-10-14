package mq

import (
	"context"
	"time"

	"go.uber.org/zap"
)

// ConsumeHandler 单条消息的处理器。返回 nil 才 ack；返回 err 时不 ack——
// 消息会在 channel/连接重建后被 broker 重投（at-least-once），
// 处理器必须自带幂等
type ConsumeHandler func(ctx context.Context, body []byte) error

// Consumer 结果队列消费器：单队列、手动 ack、断线自动重连。
// 典型用途是"唤醒信号"类消费——消息丢了无所谓（有轮询兜底），
// 重复投递也无害（处理方幂等）
type Consumer struct {
	client   *Client
	queue    string
	prefetch int
}

func (c *Client) NewConsumer(queue string, prefetch int) *Consumer {
	if prefetch <= 0 {
		prefetch = 10
	}
	return &Consumer{client: c, queue: queue, prefetch: prefetch}
}

// Run 阻塞消费循环（调用方放独立 goroutine）：连接 → Qos → Consume →
// 逐条交给 handler；出错退避重连，直到 ctx 取消。
// 连接被 Client 的重连逻辑恢复后，这里下一轮 loop 会自然接上
func (cons *Consumer) Run(ctx context.Context, h ConsumeHandler) {
	backoff := time.Second
	const maxBackoff = 30 * time.Second
	for {
		n, err := cons.consumeOnce(ctx, h)
		if ctx.Err() != nil {
			return
		}
		if n > 0 || err != nil {
			zap.L().Warn("mq consumer 通道断开，退避重连",
				zap.String("queue", cons.queue),
				zap.Int64("processed", n),
				zap.Error(err), zap.Duration("backoff", backoff))
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// consumeOnce 一条连接上的消费会话。返回处理条数与致命错误
func (cons *Consumer) consumeOnce(ctx context.Context, h ConsumeHandler) (int64, error) {
	cons.client.connMu.RLock()
	conn := cons.client.conn
	cons.client.connMu.RUnlock()
	if conn == nil || conn.IsClosed() {
		return 0, context.DeadlineExceeded // 无连接：交给上层退避重试
	}
	ch, err := conn.Channel()
	if err != nil {
		return 0, err
	}
	defer ch.Close()

	if err := ch.Qos(cons.prefetch, 0, false); err != nil {
		return 0, err
	}
	msgs, err := ch.Consume(cons.queue, "", false /*手动 ack*/, false, false, false, nil)
	if err != nil {
		return 0, err
	}

	var processed int64
	for {
		select {
		case <-ctx.Done():
			return processed, nil
		case msg, ok := <-msgs:
			if !ok {
				return processed, context.Canceled // channel 关闭：连接层出问题
			}
			if err := h(ctx, msg.Body); err != nil {
				// 不 ack：重投交给 broker。记日志别吞
				zap.L().Warn("mq consumer 处理失败，等待重投",
					zap.String("queue", cons.queue), zap.Error(err))
				_ = msg.Nack(false, true) // 立即重回队列（唤醒类消费重投无害）
				continue
			}
			_ = msg.Ack(false)
			processed++
		}
	}
}
