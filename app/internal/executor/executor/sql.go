package executor

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"job-scheduler/app/internal/domain/coordination"
	"job-scheduler/app/internal/domain/task"

	_ "github.com/go-sql-driver/mysql" // 注册 mysql 驱动
)

// SQLExecutor 执行 SQL 任务：read_only → 查询带回行；否则按 DML 执行。
// 连接每条消息独立打开（DSN 在配置快照里），结束即关，执行器自身不保连接状态
type SQLExecutor struct{}

func NewSQLExecutor() *SQLExecutor { return &SQLExecutor{} }

// maxQueryRows 查询结果带回的行数上限
const maxQueryRows = 100

func (s *SQLExecutor) Execute(ctx context.Context, msg *coordination.ExecutionMessage) ([]byte, error) {
	var cfg task.SQLConfig
	if err := json.Unmarshal(msg.Config, &cfg); err != nil {
		return nil, fmt.Errorf("executor: sql config unmarshal: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("executor: sql config invalid: %w", err)
	}

	db, err := sql.Open("mysql", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("executor: sql open: %w", err)
	}
	defer db.Close()

	start := time.Now()
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("executor: sql ping: %w", err)
	}

	var detail any
	if cfg.ReadOnly {
		rows, err := db.QueryContext(ctx, cfg.Query)
		if err != nil {
			return nil, fmt.Errorf("executor: sql query: %w", err)
		}
		defer rows.Close()

		cols, err := rows.Columns()
		if err != nil {
			return nil, fmt.Errorf("executor: sql columns: %w", err)
		}
		results := make([]map[string]any, 0, maxQueryRows)
		for rows.Next() && len(results) < maxQueryRows {
			values := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range values {
				ptrs[i] = &values[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				return nil, fmt.Errorf("executor: sql scan: %w", err)
			}
			row := make(map[string]any, len(cols))
			for i, c := range cols {
				row[c] = scanValue(values[i])
			}
			results = append(results, row)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("executor: sql rows: %w", err)
		}
		detail = sqlDetail{Rows: results}
	} else {
		res, err := db.ExecContext(ctx, cfg.Query)
		if err != nil {
			return nil, fmt.Errorf("executor: sql exec: %w", err)
		}
		affected, _ := res.RowsAffected()
		detail = sqlDetail{Affected: affected}
	}
	return marshalResult(execResult{
		OK:         true,
		DurationMS: since(start),
		Detail:     detail,
	}), nil
}

// scanValue driver 返回的 []byte 转 string，其余原样（保证 JSON 可序列化）
func scanValue(v any) any {
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return v
}

type sqlDetail struct {
	Rows     []map[string]any `json:"rows,omitempty"`
	Affected int64            `json:"affected,omitempty"`
}
