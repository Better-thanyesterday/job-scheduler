package mq

import (
	"context"
	"fmt"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

// Producer 生产者
type Producer struct {
	client *Client
}

func (c *Client) NewProducer() *Producer {
	return &Producer{
		client: c,
	}
}

// getChannel 从当前连接开一条新信道（ConfirmMode 时启用 confirm）。
// 信道生命周期归调用方：用完自行 Close
func (p *Producer) getChannel() (*amqp091.Channel, error) {
	p.client.connMu.RLock()
	conn := p.client.conn
	p.client.connMu.RUnlock()
	if conn == nil || conn.IsClosed() {
		zap.L().Info("mq connection not available")
		return nil, fmt.Errorf("mq connection not available")
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("create channel failed: %w", err)
	}
	if p.client.cfg.ConfirmMode {
		if err := ch.Confirm(false); err != nil {
			_ = ch.Close()
			return nil, fmt.Errorf("enable confirm mode failed: %w", err)
		}
	}
	return ch, nil
}

// PublishRaw 投递任意载荷（消息任务等业务载荷用，不携带执行语义字段）。
// routingKey 约定为 topic 名；走 confirm 模式时同样等待 broker 确认
func (p *Producer) PublishRaw(ctx context.Context, routingKey string, body []byte) error {
	ch, err := p.getChannel()
	if err != nil {
		return err
	}
	defer ch.Close()
	pub := amqp091.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp091.Persistent,
		Body:         body,
		Timestamp:    time.Now(),
	}
	if !p.client.cfg.ConfirmMode {
		return ch.PublishWithContext(ctx, ExchangeDirect, routingKey, false, false, pub)
	}
	confirms := ch.NotifyPublish(make(chan amqp091.Confirmation))
	if err := ch.PublishWithContext(ctx, ExchangeDirect, routingKey, false, false, pub); err != nil {
		return fmt.Errorf("publish raw failed: %w", err)
	}
	select {
	case confirm := <-confirms:
		if !confirm.Ack {
			return fmt.Errorf("message nacked by broker")
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("publish raw confirm timeout: %w", ctx.Err())
	case <-time.After(p.client.cfg.ConfirmTimeout):
		return fmt.Errorf("publish raw confirm timeout: %w", ctx.Err())
	}
}
