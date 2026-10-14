// Package resultstore 执行结果的溢出存储：
// 结果超过内联阈值时，executor 把全量结果 gzip 后直传 MinIO，
// 数据库里只落一个自描述信封（key/size/sha256/preview）——result 列仍是 JSON，
// 读方不感知存储后端。
//
// 防孤儿约定：对象 key 由 exec_id 确定化生成（重传覆盖同 key，天然幂等），
// GC 周期扫描"DB 无引用且超过宽限期"的对象兜底清理。
package resultstore

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/lifecycle"
	"go.uber.org/zap"
)

// 结果对象保留期：MinIO ILM 按 executions/ 前缀自动过期，启动时幂等下发
const ResultTTLDays = 30

// previewBytes 信封里内联的结果预览长度：详情页不碰对象存储就能看到开头
const previewBytes = 4096

// Config MinIO 连接与桶配置（来自 configs 的 executor.minio 段）
type Config struct {
	Endpoint  string `mapstructure:"endpoint"` // 如 "127.0.0.1:9000"
	AccessKey string `mapstructure:"access_key"`
	SecretKey string `mapstructure:"secret_key"`
	Bucket    string `mapstructure:"bucket"`  // 默认 job-results
	UseSSL    bool   `mapstructure:"use_ssl"` // 内网 http 部署保持 false
}

// Store MinIO 溢出存储。方法失败一律返回 error，由调用方决定降级
type Store struct {
	cli    *minio.Client
	bucket string
}

func New(cfg Config) (*Store, error) {
	cli, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("resultstore: minio client: %w", err)
	}
	bucket := cfg.Bucket
	if bucket == "" {
		bucket = "job-results"
	}
	return &Store{cli: cli, bucket: bucket}, nil
}

// BucketName 信封里的 bucket 字段取值
func (s *Store) BucketName() string { return s.bucket }

// EnsureSetup 启动时幂等执行：建桶 + 下发 30 天过期规则（executions/ 前缀）
func (s *Store) EnsureSetup(ctx context.Context) error {
	exists, err := s.cli.BucketExists(ctx, s.bucket)
	if err != nil {
		return fmt.Errorf("resultstore: bucket check: %w", err)
	}
	if !exists {
		if err := s.cli.MakeBucket(ctx, s.bucket, minio.MakeBucketOptions{}); err != nil {
			return fmt.Errorf("resultstore: make bucket: %w", err)
		}
	}
	cfg := lifecycle.NewConfiguration()
	cfg.Rules = []lifecycle.Rule{{
		ID:         "expire-execution-results",
		Status:     "Enabled",
		RuleFilter: lifecycle.Filter{Prefix: "executions/"},
		Expiration: lifecycle.Expiration{Days: lifecycle.ExpirationDays(ResultTTLDays)},
	}}
	if err := s.cli.SetBucketLifecycle(ctx, s.bucket, cfg); err != nil {
		return fmt.Errorf("resultstore: set lifecycle: %w", err)
	}
	return nil
}

// KeyFor 结果对象 key：exec_id 确定化——重传覆盖同 key，天然幂等
func KeyFor(execID int64) string {
	return path.Join("executions", strconv.FormatInt(execID, 10), "result.json.gz")
}

// Store 全量结果入库对象存储：gzip → sha256 → 上传 → 返回信封 JSON。
// result 原文不做任何修改，预览取开头 previewBytes
func (s *Store) Store(ctx context.Context, execID int64, result []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(result); err != nil {
		return nil, fmt.Errorf("resultstore: gzip: %w", err)
	}
	if err := zw.Close(); err != nil {
		return nil, fmt.Errorf("resultstore: gzip close: %w", err)
	}
	sum := sha256.Sum256(result)
	key := KeyFor(execID)
	compressed := buf.Len() // PutObject 会消费 buffer，长度必须先取
	_, err := s.cli.PutObject(ctx, s.bucket, key, &buf, int64(compressed),
		minio.PutObjectOptions{ContentType: "application/gzip"})
	if err != nil {
		return nil, fmt.Errorf("resultstore: put object: %w", err)
	}
	return BuildEnvelope(s.bucket, key, compressed, hex.EncodeToString(sum[:]), result, previewBytes), nil
}

// 引用检查回调：返回该 exec 的 result 列是否引用了指定对象 key。
// GC 用，由调用方（持有 DB 的 main）注入，本包不依赖数据库
type ReferencedFunc func(ctx context.Context, execID int64, key string) (bool, error)

// RunGC 孤儿对象回收：扫描 executions/ 前缀，对象超过宽限期仍无 DB 引用即删除。
// 宽限期覆盖"先上传后写库"窗口内的正常在途对象
func (s *Store) RunGC(ctx context.Context, referenced ReferencedFunc, interval, grace time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.gcOnce(ctx, referenced, grace)
		}
	}
}

func (s *Store) gcOnce(ctx context.Context, referenced ReferencedFunc, grace time.Duration) {
	objects := s.cli.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: "executions/", Recursive: true})
	for obj := range objects {
		if obj.Err != nil {
			zap.L().Warn("resultstore: GC 列举对象失败", zap.Error(obj.Err))
			continue
		}
		if time.Since(obj.LastModified) <= grace {
			continue
		}
		execID, ok := ExecIDFromKey(obj.Key)
		if !ok {
			continue
		}
		ref, err := referenced(ctx, execID, obj.Key)
		if err != nil {
			zap.L().Warn("resultstore: GC 查引用失败", zap.String("key", obj.Key), zap.Error(err))
			continue
		}
		if ref {
			continue
		}
		if err := s.cli.RemoveObject(ctx, s.bucket, obj.Key, minio.RemoveObjectOptions{}); err != nil {
			zap.L().Warn("resultstore: GC 删除孤儿对象失败", zap.String("key", obj.Key), zap.Error(err))
			continue
		}
		zap.L().Info("resultstore: 已回收孤儿结果对象", zap.String("key", obj.Key))
	}
}

// ---------- 纯函数（可独立单测） ----------

// Envelope result 列里溢出结果的信封形态。自描述 JSON，
// 不认识它的旧读方也只是看到一段普通 JSON，前向兼容
type Envelope struct {
	Storage  string `json:"storage"` // 恒 "minio"
	Bucket   string `json:"bucket"`
	Key      string `json:"key"`
	Size     int    `json:"size"`      // 压缩后字节数
	OrigSize int    `json:"orig_size"` // 原文字节数
	SHA256   string `json:"sha256"`    // 原文校验和
	Preview  string `json:"preview"`   // 原文开头（详情页零依赖展示）
	StoredAt string `json:"stored_at"` // RFC3339
	TTLDays  int    `json:"ttl_days"`  // 对象保留期，供读方提示
}

// BuildEnvelope 组装信封 JSON。纯函数：hash 由调用方算好传入
func BuildEnvelope(bucket, key string, compressedSize int, sha256hex string, result []byte, previewLen int) []byte {
	env := Envelope{
		Storage:  "minio",
		Bucket:   bucket,
		Key:      key,
		Size:     compressedSize,
		OrigSize: len(result),
		SHA256:   sha256hex,
		Preview:  HeadTail(string(result), previewLen),
		StoredAt: time.Now().UTC().Format(time.RFC3339),
		TTLDays:  ResultTTLDays,
	}
	b, err := json.Marshal(env)
	if err != nil {
		return []byte(`{"storage":"minio","error":"marshal envelope failed"}`)
	}
	return b
}

// HeadTail 超长文本保留头尾、中间省略：失败排查的关键信息几乎总在两端
func HeadTail(s string, each int) string {
	if len(s) <= each*2 {
		return s
	}
	return s[:each] + "\n...(中间省略 " + strconv.Itoa(len(s)-each*2) + " 字节，完整结果见对象存储)...\n" + s[len(s)-each:]
}

// ExecIDFromKey 从对象 key 反解 exec_id；非结果对象返回 false
func ExecIDFromKey(key string) (int64, bool) {
	parts := strings.Split(key, "/")
	if len(parts) != 3 || parts[0] != "executions" {
		return 0, false
	}
	id, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || id <= 0 {
		return 0, false
	}
	return id, true
}

// ResultReferences 引用检查的兜底实现（DB 侧在 main 里组装）：
// result 列原文包含 key 即视为引用
func ResultReferences(resultColumn string, key string) bool {
	return strings.Contains(resultColumn, key)
}

// PresignGet 为信封对象生成预签名下载 URL（默认有效期 15 分钟）：
// api 侧"下载完整结果"端点用——api 自己不依赖 MinIO 拉数据，
// 302 给用户直连对象存储，大载荷不过 api。
// MinIO 预签名是本地 HMAC 计算，不产生网络往返
func (s *Store) PresignGet(ctx context.Context, env Envelope, expiry time.Duration) (string, error) {
	u, err := s.cli.PresignedGetObject(ctx, env.Bucket, env.Key, expiry, nil)
	if err != nil {
		return "", fmt.Errorf("resultstore: presign %s: %w", env.Key, err)
	}
	return u.String(), nil
}

// ParseEnvelope 判断一段 result 是否为溢出信封并解析。
// 任意普通 JSON（如 {"ok":true}）与非 JSON 一律返回 false——
// 读方对每个上游输出调本函数即可放心区分"信封"与"内联原文"
func ParseEnvelope(b []byte) (Envelope, bool) {
	var env Envelope
	if err := json.Unmarshal(b, &env); err != nil {
		return env, false
	}
	if env.Storage != "minio" || env.Key == "" {
		return env, false
	}
	return env, true
}

// Load 信封回源：入参是信封 JSON → 从对象存储拉取、解压、三重校验
// （gzip 完整性 / 原文长度 / sha256）后返回全量原文；
// 入参不是信封（普通内联结果）→ 原样透传。
// A→B 数据流的消费方对每个上游输出调一次本方法即可，
// 无需自己判断是否溢出——这是溢出信封与 A→B 直传并存的关键一环
func (s *Store) Load(ctx context.Context, resultOrEnvelope []byte) ([]byte, error) {
	env, ok := ParseEnvelope(resultOrEnvelope)
	if !ok {
		return resultOrEnvelope, nil
	}
	obj, err := s.cli.GetObject(ctx, env.Bucket, env.Key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("resultstore: get object %s: %w", env.Key, err)
	}
	defer obj.Close()
	compressed, err := io.ReadAll(obj)
	if err != nil {
		return nil, fmt.Errorf("resultstore: read object %s: %w", env.Key, err)
	}
	return decompress(compressed, env)
}

// decompress 信封对应的压缩对象 → 原文：gunzip + 长度 + sha256 校验。
// 任一不过说明对象损坏/被篡改/过期错配，绝不把可疑字节当执行结果返回
func decompress(compressed []byte, env Envelope) ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, fmt.Errorf("resultstore: gzip reader %s: %w", env.Key, err)
	}
	full, err := io.ReadAll(zr)
	if err != nil {
		return nil, fmt.Errorf("resultstore: gunzip %s: %w", env.Key, err)
	}
	if len(full) != env.OrigSize {
		return nil, fmt.Errorf("resultstore: size mismatch %s: got %d want %d",
			env.Key, len(full), env.OrigSize)
	}
	sum := sha256.Sum256(full)
	if hex.EncodeToString(sum[:]) != env.SHA256 {
		return nil, fmt.Errorf("resultstore: sha256 mismatch %s", env.Key)
	}
	return full, nil
}
