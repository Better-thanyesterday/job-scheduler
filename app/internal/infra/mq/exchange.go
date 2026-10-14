package mq

import (
	"go.uber.org/zap"
)

// ExchangeDirect 结果回执路由的 direct 交换机（dag 引擎唤醒消费）
const ExchangeDirect = "job.direct"

func (c *Client) DeclareExchanges() error {
	ch, err := c.getMgmtChannel()
	if err != nil {
		zap.L().Info("declarexchange get mgmtchannel fail:", zap.Error(err))
		return err
	}
	if err := ch.ExchangeDeclare(ExchangeDirect, "direct", true, false, false, false, nil); err != nil {
		zap.L().Info("declare direct exchange failed:", zap.Error(err))
		return err
	}
	return nil
}

// DeclareAll 一键声明所有交换机和队列
func (c *Client) DeclareAll() error {
	if err := c.DeclareExchanges(); err != nil {
		return err
	}
	return c.DeclareQueues()
}
