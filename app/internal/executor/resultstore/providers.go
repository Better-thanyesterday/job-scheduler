package resultstore

import "job-scheduler/app/settings"

// ProvideStore 结果溢出存储。api 侧只用于生成预签名下载 URL（不拉数据）；
// executor 侧直接 resultstore.New（需要区分生命周期时也用本 provider）。
// 未配置 executor.minio 返回 nil——溢出相关能力届时报明确错误
func ProvideStore(cfg *settings.AppConfig) *Store {
	exe := cfg.ExecutorConfig
	if exe == nil || exe.Minio == nil {
		return nil
	}
	store, err := New(Config{
		Endpoint:  exe.Minio.Endpoint,
		AccessKey: exe.Minio.AccessKey,
		SecretKey: exe.Minio.SecretKey,
		Bucket:    exe.Minio.Bucket,
		UseSSL:    exe.Minio.UseSSL,
	})
	if err != nil {
		return nil
	}
	return store
}
