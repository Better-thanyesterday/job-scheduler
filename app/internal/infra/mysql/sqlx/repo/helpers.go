package sqlx

import "time"

// 本包共享的值转换函数。execution 与 dag 的写路径语义相同，抽到这里
// 避免同包重名；每个函数的 NULL 语义与 gorm 版 repo 一致。

// timeValue 零值时间写 NULL：datetime 严格模式（NO_ZERO_DATE）不接受 '0000-00-00'
func timeValue(t time.Time) interface{} {
	if t.IsZero() {
		return nil
	}
	return t
}

// nextRetryValue 指针时间：nil 写 NULL（executions.next_retry_at）
func nextRetryValue(t *time.Time) interface{} {
	if t == nil {
		return nil
	}
	return *t
}

// resultValue 空结果写 NULL：json 列不接受空串
func resultValue(b []byte) interface{} {
	if len(b) == 0 {
		return nil
	}
	return string(b)
}

// execValue 未派发的节点 execution_id 写 NULL
func execValue(execID int64) interface{} {
	if execID <= 0 {
		return nil
	}
	return execID
}

// jsonOrNull 空字符串视为 NULL（json 列不接受空串）
func jsonOrNull(s string) []byte {
	if s == "" {
		return nil
	}
	return []byte(s)
}
