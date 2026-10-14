package settings

import (
	"fmt"
	"os"
	"sync/atomic"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
)

// Conf 原子指针，存放 *AppConfig 快照，只读，禁止原地修改
var Conf atomic.Pointer[AppConfig]

type AppConfig struct {
	Name      string `mapstructure:"name"`
	Mode      string `mapstructure:"mode"`
	Version   string `mapstructure:"version"`
	StartTime string `mapstructure:"start_time"`
	MachineID int64  `mapstructure:"machine_id"`
	Port      int    `mapstructure:"port"`

	*LogConfig  `mapstructure:"log"`
	MySqls      map[string]*MySQLConfig `mapstructure:"mysqls"`
	*MQConfig   `mapstructure:"mq"`
	*EtcdConfig `mapstructure:"etcd"`
	*AuthConfig `mapstructure:"auth"`

	// executor 推送模式的专属配置（executor 进程读）
	*ExecutorConfig `mapstructure:"executor"`

	MetricsPort int    `mapstructure:"metrics_port"`
	WorkerID    string `mapstructure:"worker_id"`
}

// AuthConfig JWT 签发配置（api 进程读）。
// jwt_secret 是签名密钥：泄露即可伪造任意用户 token，生产必改且不入库
type AuthConfig struct {
	JwtSecret string `mapstructure:"jwt_secret"`
	JwtExpire int    `mapstructure:"jwt_expire"` // token 有效期（小时），默认 72
}

// EtcdConfig 执行器注册/发现共用的 etcd 连接
type EtcdConfig struct {
	Endpoints []string `mapstructure:"endpoints"`
}

// ExecutorConfig executor 进程的 gRPC 服务与上报配置
type ExecutorConfig struct {
	Listen      string `mapstructure:"listen"`       // gRPC 监听地址，如 ":9901"
	Advertise   string `mapstructure:"advertise"`    // 上报给 etcd 的可达地址；空则从 listen 推导（跨机部署必配）
	LeaseTTL    int64  `mapstructure:"lease_ttl"`    // etcd 注册租约秒数，默认 15
	MetricsPort int    `mapstructure:"metrics_port"` // /metrics 端口，默认 9902（与 scheduler 的 9100 区分）
	MaxConcurre int32  `mapstructure:"max_concurrency"`
	Version     string `mapstructure:"version"`

	// ResultInlineMaxBytes 结果内联阈值：超过它的成功结果全量直传 MinIO（三段式溢出），
	// 0 = 默认 32KB。未配 minio 段时本值无效（维持 4KB 截断）
	ResultInlineMaxBytes int `mapstructure:"result_inline_max_bytes"`
	// Minio 结果溢出存储后端；不配置 = 不启用溢出（保持截断内联）
	Minio *MinioConfig `mapstructure:"minio"`
}

// MinioConfig 结果溢出存储的 MinIO/S3 连接配置。
// 凭据属敏感信息，不要把真实 secret 提交进仓库
type MinioConfig struct {
	Endpoint  string `mapstructure:"endpoint"` // 如 "127.0.0.1:9000"
	AccessKey string `mapstructure:"access_key"`
	SecretKey string `mapstructure:"secret_key"`
	Bucket    string `mapstructure:"bucket"`  // 缺省 job-results
	UseSSL    bool   `mapstructure:"use_ssl"` // 内网 http 部署保持 false
}

// AdvertiseAddr 上报地址：未显式配置时从监听地址推导（监听 :9901 → 127.0.0.1:9901，
// 单机可跑；跨机部署必须显式配 advertise）
func (c *ExecutorConfig) AdvertiseAddr() string {
	if c.Advertise != "" {
		return c.Advertise
	}
	if len(c.Listen) > 0 && c.Listen[0] == ':' {
		return "127.0.0.1" + c.Listen
	}
	return c.Listen
}

type MQConfig struct {
	Host          string `mapstructure:"host"`
	Port          int    `mapstructure:"port"`
	User          string `mapstructure:"user"`
	Password      string `mapstructure:"password"`
	VHost         string `mapstructure:"vhost"`
	ConfirmMode   bool   `mapstructure:"confirm_mode"`
	PrefetchCount int    `mapstructure:"prefetch_count"`
}

type MySQLConfig struct {
	Host         string `mapstructure:"host"`
	User         string `mapstructure:"user"`
	Password     string `mapstructure:"password"`
	DB           string `mapstructure:"dbname"`
	Port         int    `mapstructure:"port"`
	MaxOpenConns int    `mapstructure:"max_open_conns"`
	MaxIdleConns int    `mapstructure:"max_idle_conns"`
}

type LogConfig struct {
	Level      string `mapstructure:"level"`
	MaxSize    int    `mapstructure:"max_size"`
	MaxAge     int    `mapstructure:"max_age"`
	MaxBackups int    `mapstructure:"max_backups"`
}

// loadNewConfig 加载并返回一份全新的配置快照（每次都新建，不复用旧结构体）
func loadNewConfig() (*AppConfig, error) {
	var cfg AppConfig
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func Init() error {
	// 配置路径：CONFIG_PATH 环境变量优先，缺省相对当前工作目录 ./configs/config.yaml
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "./configs/config.yaml"
	}

	viper.SetConfigFile(configPath)

	if err := viper.ReadInConfig(); err != nil {
		fmt.Printf("viper.ReadInConfig failed, err:%v\n", err)
		return err
	}

	initialCfg, err := loadNewConfig()
	if err != nil {
		fmt.Printf("viper.Unmarshal initial config failed, err:%v\n", err)
		return err
	}
	Conf.Store(initialCfg)

	viper.WatchConfig()
	viper.OnConfigChange(func(in fsnotify.Event) {
		fmt.Println("配置文件修改了...")
		newCfg, err := loadNewConfig()
		if err != nil {
			fmt.Printf("热加载viper.Unmarshal failed, err:%v\n", err)
			// 解析失败：保留旧配置，不替换
			return
		}
		// 原子替换快照，所有读协程自动切换到新版本
		Conf.Store(newCfg)
		fmt.Println("配置热加载成功")
	})
	return nil
}
