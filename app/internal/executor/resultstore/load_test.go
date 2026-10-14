package resultstore

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
)

// 回源语义：信封 → 全量原文（解压+校验）；非信封 → 原样透传。
// 这是"溢出信封与 A→B 直传并存"的关键保证，坏一条数据流就断一条
func TestLoadPassthroughAndResolve(t *testing.T) {
	original := []byte(`{"very":"large result payload","n":1234567890}`)
	sum := sha256.Sum256(original)

	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	if _, err := zw.Write(original); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	envelope := BuildEnvelope("job-results", "executions/42/result.json.gz",
		compressed.Len(), hex.EncodeToString(sum[:]), original, 64)

	// 1. ParseEnvelope：信封识别 + 非 JSON/普通 JSON 不误判
	if env, ok := ParseEnvelope(envelope); !ok || env.Key != "executions/42/result.json.gz" {
		t.Fatalf("信封应被识别: ok=%v env=%+v", ok, env)
	}
	for _, not := range [][]byte{
		original,                      // 普通 JSON（结果原文常见形态）
		[]byte("plain text output"),   // 非 JSON
		[]byte(`{"storage":"minio"}`), // 缺 key 不算信封
		nil,
	} {
		if _, ok := ParseEnvelope(not); ok {
			t.Fatalf("非信封 %q 不应被识别", not)
		}
	}

	// 2. 透传语义本函数级验证：非信封原样返回
	// （Store.Load 的透传分支依赖同一 ParseEnvelope 判定）

	// 3. decompress：gunzip + 长度 + sha256 三重校验后还原原文
	env, _ := ParseEnvelope(envelope)
	got, err := decompress(compressed.Bytes(), env)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("回源原文不匹配: got %q want %q", got, original)
	}

	// 4. 损坏检测：翻转一个字节必须报错，绝不返回可疑数据
	corrupted := append([]byte(nil), compressed.Bytes()...)
	corrupted[len(corrupted)/2] ^= 0xFF
	if _, err := decompress(corrupted, env); err == nil {
		t.Fatal("损坏对象必须回源失败")
	}
	// 长度不符
	if _, err := decompress(compressed.Bytes()[:compressed.Len()-1], env); err == nil {
		t.Fatal("截断对象必须回源失败")
	}
}

// Store.Load 的透传分支：非信封原样返回（无 MinIO 也可测，不触发网络）
func TestStoreLoadPassthrough(t *testing.T) {
	s := &Store{}
	raw := []byte(`{"inline":"result"}`)
	got, err := s.Load(context.Background(), raw)
	if err != nil {
		t.Fatalf("非信封透传不应报错: %v", err)
	}
	if !bytes.Equal(got, raw) {
		t.Fatalf("透传内容被改动: %q", got)
	}
	// 信封但没有 MinIO 客户端：应当报错而不是 panic
	env := BuildEnvelope("b", "executions/1/result.json.gz", 1, "x", []byte("y"), 1)
	if _, err := s.Load(context.Background(), env); err == nil {
		t.Fatal("无客户端回源信封应报错")
	}
}

// 信封必须自描述可解析：marshal 出来的就是 ParseEnvelope 能认回的
func TestEnvelopeJSONRoundTrip(t *testing.T) {
	env := BuildEnvelope("job-results", "executions/9/result.json.gz", 5,
		"deadbeef", []byte("hello world, this is the original"), 16)
	parsed, ok := ParseEnvelope(env)
	if !ok {
		t.Fatalf("BuildEnvelope 产物应能被 ParseEnvelope 认回: %s", env)
	}
	if parsed.Bucket != "job-results" || parsed.OrigSize != len("hello world, this is the original") {
		t.Fatalf("信封字段不符: %+v", parsed)
	}
	if !json.Valid(env) {
		t.Fatal("信封必须是合法 JSON")
	}
}
