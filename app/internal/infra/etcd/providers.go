package etcd

import (
	"errors"

	"go.etcd.io/etcd/client/v3"

	"job-scheduler/app/settings"
)

// ProvideConfig 从应用配置快照里取 etcd 配置段
func ProvideConfig(cfg *settings.AppConfig) *settings.EtcdConfig { return cfg.EtcdConfig }

// ProvideClient wire 用 provider：从配置建 etcd 客户端，cleanup 负责关闭连接。
// api 服务手动触发执行需要它（经 pusher 按 etcd 成员表发现 executor）
func ProvideClient(cfg *settings.EtcdConfig) (*clientv3.Client, func(), error) {
	if cfg == nil || len(cfg.Endpoints) == 0 {
		return nil, nil, errors.New("etcd config missing (etcd.endpoints)")
	}
	cli, err := NewEtcdClient(cfg.Endpoints)
	if err != nil {
		return nil, nil, err
	}
	return cli, func() { _ = cli.Close() }, nil
}
