package resultstore

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestKeyForDeterministic(t *testing.T) {
	if KeyFor(42) != "executions/42/result.json.gz" {
		t.Fatalf("KeyFor(42) = %q", KeyFor(42))
	}
	if KeyFor(42) != KeyFor(42) {
		t.Fatal("同 exec_id 的 key 必须确定（重传覆盖同 key 幂等）")
	}
}

func TestExecIDFromKey(t *testing.T) {
	if id, ok := ExecIDFromKey("executions/123/result.json.gz"); !ok || id != 123 {
		t.Fatalf("ExecIDFromKey 正常 key 解析失败: %d %v", id, ok)
	}
	for _, bad := range []string{"other/123/result.json.gz", "executions/abc/result.json.gz", "executions/0/result.json.gz", "executions/123"} {
		if _, ok := ExecIDFromKey(bad); ok {
			t.Fatalf("ExecIDFromKey(%q) 应当返回 false", bad)
		}
	}
}

func TestBuildEnvelope(t *testing.T) {
	result := []byte(strings.Repeat("x", 10000))
	env := BuildEnvelope("job-results", "executions/7/result.json.gz", 120,
		"abc123", result, 4096)
	var e struct {
		Storage  string `json:"storage"`
		Bucket   string `json:"bucket"`
		Key      string `json:"key"`
		Size     int    `json:"size"`
		OrigSize int    `json:"orig_size"`
		SHA256   string `json:"sha256"`
		Preview  string `json:"preview"`
		TTLDays  int    `json:"ttl_days"`
	}
	if err := json.Unmarshal(env, &e); err != nil {
		t.Fatal(err)
	}
	if e.Storage != "minio" || e.Bucket != "job-results" || e.Key != "executions/7/result.json.gz" {
		t.Fatalf("信封寻址字段错误: %+v", e)
	}
	if e.Size != 120 || e.OrigSize != 10000 || e.SHA256 != "abc123" {
		t.Fatalf("信封计量字段错误: %+v", e)
	}
	if len(e.Preview) > 2*4096+100 {
		t.Fatalf("preview 应受预览长度约束: %d", len(e.Preview))
	}
	if e.TTLDays != ResultTTLDays || ResultTTLDays != 30 {
		t.Fatalf("TTL 应为 30 天: %+v", e)
	}
}

func TestHeadTail(t *testing.T) {
	short := "hello"
	if HeadTail(short, 3) != short {
		t.Fatal("短文本不应被加工")
	}
	long := strings.Repeat("a", 100) + "MIDDLE" + strings.Repeat("b", 100)
	got := HeadTail(long, 10)
	if !strings.HasPrefix(got, strings.Repeat("a", 10)) || !strings.HasSuffix(got, strings.Repeat("b", 10)) {
		t.Fatal("head/tail 保留错误")
	}
	if !strings.Contains(got, "中间省略") {
		t.Fatal("省略标记缺失")
	}
}

func TestResultReferences(t *testing.T) {
	col := `{"storage":"minio","key":"executions/9/result.json.gz"}`
	if !ResultReferences(col, "executions/9/result.json.gz") {
		t.Fatal("引用中的 key 应判定为在用")
	}
	if ResultReferences(col, "executions/8/result.json.gz") {
		t.Fatal("无关 key 不应在用")
	}
	if ResultReferences("", "executions/9/result.json.gz") {
		t.Fatal("空列不应在用")
	}
}
