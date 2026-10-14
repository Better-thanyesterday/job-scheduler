package mq

import (
	"fmt"
	"sync"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type Config struct {
	Host           string        `yaml:"host"`
	Port           int           `yaml:"port"`
	Password       string        `yaml:"password"`
	VHost          string        `yaml:"vhost"`
	User           string        `yaml:"user"`
	Heartbeat      time.Duration `yaml:"heartbeat"`
	ReconnectDelay time.Duration `yaml:"reconnectdelay"`
	MaxReconnect   int           `yaml:"maxreconnect"`

	// 生产者参数
	ConfirmMode    bool          `yaml:"confirm_mode"`    // 是否开启 confirm 模式
	ConfirmTimeout time.Duration `yaml:"confirm_timeout"` // confirm 等待超时，默认 5s

	// 消费者参数
	PrefetchCount int  `yaml:"prefetch_count"` // 默认 10
	PrefetchSize  int  `yaml:"prefetch_size"`  // 默认 0
	GlobalQos     bool `yaml:"global_qos"`     // 默认 false
}

func (c *Config) Addr() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%d/%s",
		c.User, c.Password, c.Host, c.Port, c.VHost)
}

// Client MQ 客户端
type Client struct {
	cfg    *Config
	conn   *amqp091.Connection
	connMu sync.RWMutex

	// 管理 Channel（用于声明队列、交换机，不用于消费）
	mgmtCh *amqp091.Channel
	mgmtMu sync.Mutex

	// 生产者 Channel 池
	prodChs chan *amqp091.Channel
	prodMu  sync.Mutex

	// 关闭信号
	closeCh chan struct{}
	once    sync.Once
	wg      sync.WaitGroup
}

func NewClient(cfg *Config) (*Client, error) {
	if cfg.Heartbeat == 0 {
		cfg.Heartbeat = 10 * time.Second
	}
	if cfg.ReconnectDelay == 0 {
		cfg.ReconnectDelay = 3 * time.Second
	}
	if cfg.MaxReconnect == 0 {
		cfg.MaxReconnect = 10
	}
	if cfg.ConfirmTimeout == 0 {
		cfg.ConfirmTimeout = 5 * time.Second
	}
	if cfg.PrefetchCount == 0 {
		cfg.PrefetchCount = 10
	}
	c := &Client{
		cfg:     cfg,
		closeCh: make(chan struct{}),
		prodChs: make(chan *amqp091.Channel),
	}
	if err := c.connect(); err != nil {
		zap.L().Info("mq initial connect failed:", zap.Error(err))
		return nil, err
	}
	c.wg.Add(1)
	go c.connectionDaemon()
	return c, nil
}

func (c *Client) Close() error {
	c.once.Do(func() {
		close(c.closeCh)
	})
	c.wg.Wait()
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if c.mgmtCh != nil {
		_ = c.mgmtCh.Close()
	}
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *Client) connect() error {
	cfg := amqp091.Config{
		Heartbeat: c.cfg.Heartbeat,
	}
	conn, err := amqp091.DialConfig(c.cfg.Addr(), cfg)
	if err != nil {
		zap.L().Info("dail config fail", zap.Error(err))
		return err
	}
	ch, err := conn.Channel()
	if err != nil {
		zap.L().Info("channel  fail", zap.Error(err))
		_ = conn.Close()
		return err
	}
	c.connMu.Lock()
	c.conn = conn
	c.mgmtCh = ch
	c.connMu.Unlock()
	return nil
}

// connectionDaemon 连接守护：监听连接关闭事件，自动重连
func (c *Client) connectionDaemon() {
	defer c.wg.Done()
	for {
		select {
		case <-c.closeCh:
			return
		default:
		}
		c.connMu.RLock()
		conn := c.conn
		c.connMu.RUnlock()
		if conn == nil || conn.IsClosed() {
			c.reconnect()
			continue
		}
		closeNotify := make(chan *amqp091.Error, 1)
		conn.NotifyClose(closeNotify)
		select {
		case <-c.closeCh:
			return
		case <-closeNotify:
			c.reconnect()
		}
	}
}

func (c *Client) reconnect() {
	for i := 0; i < c.cfg.MaxReconnect; i++ {
		select {
		case <-c.closeCh:
			return
		case <-time.After(c.cfg.ReconnectDelay):
		}
		if err := c.connect(); err != nil {
			continue
		}
	}
}

// getMgmtChannel 获取管理 Channel（线程安全）
func (c *Client) getMgmtChannel() (*amqp091.Channel, error) {
	c.mgmtMu.Lock()
	defer c.mgmtMu.Unlock()
	if c.mgmtCh == nil || c.mgmtCh.IsClosed() {
		c.connMu.RLock()
		conn := c.conn
		c.connMu.RUnlock()
		if conn == nil || conn.IsClosed() {
			zap.L().Info("mq connection not available")
			return nil, fmt.Errorf("mq connection not available")
		}
		ch, err := conn.Channel()
		if err != nil {
			zap.L().Info("getMgmtChannel channel() fail", zap.Error(err))
			return nil, err
		}
		c.mgmtCh = ch
	}
	return c.mgmtCh, nil
}
