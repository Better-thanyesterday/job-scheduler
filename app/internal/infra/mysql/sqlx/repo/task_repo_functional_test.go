package sqlx

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql" // 注册 mysql 驱动（本包其他文件不经过 database/sql 初始化）
	"github.com/jmoiron/sqlx"

	"job-scheduler/app/internal/domain/task"
	"job-scheduler/app/internal/infra/mysql"
)

const funcTestDSN = "root:root@tcp(127.0.0.1:3306)/job_task?parseTime=true&loc=Local"

// 功能验证：TaskRepo 真库读写——创建、重名校验、首页精确 total、
// ListPage 的 has_more（不 COUNT 的分页路径）
func TestTaskRepoFunctional(t *testing.T) {
	db, err := sqlx.Connect("mysql", funcTestDSN)
	if err != nil {
		t.Skipf("MySQL 不可用，跳过真库功能测试: %v", err)
	}
	defer db.Close()

	r := NewTaskRepo(mysql.TaskDB{DB: db})
	ctx := context.Background()
	testUser := time.Now().UnixNano() // 独立用户空间，测完整体清理
	defer db.Exec("DELETE FROM tasks WHERE created_by = ?", testUser)

	mkTask := func(name string) *task.Task {
		cfg := json.RawMessage(`{"url":"http://127.0.0.1:8888/api/v1/tasks/list","method":"GET"}`)
		tk, err := task.New(time.Now().UnixNano(), name, 60, "功能测试", 1, 1, cfg, nil, testUser)
		if err != nil {
			t.Fatalf("构造任务 %s: %v", name, err)
		}
		if err := r.Create(ctx, tk); err != nil {
			t.Fatalf("落库 %s: %v", name, err)
		}
		return tk
	}

	first := mkTask("repo-functional-task")

	// 1. 重名校验命中
	ok, err := r.ExistByCreatedByWithTaskName(ctx, testUser, "repo-functional-task")
	if err != nil || !ok {
		t.Fatalf("重名校验应当命中: ok=%v err=%v", ok, err)
	}

	// 2. 首页：精确 total（COUNT 路径）
	conds := task.Conds{CreateBy: testUser}
	items, total, err := r.List(ctx, 0, 10, conds)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if total != 1 || len(items) != 1 {
		t.Fatalf("首页 total=%d items=%d, 期望 1/1", total, len(items))
	}
	if items[0].ID() != first.ID() {
		t.Fatalf("列表任务 ID = %d, 期望 %d", items[0].ID(), first.ID())
	}

	// 3. ListPage 第二页：无 COUNT，has_more=false
	page2, hasMore, err := r.ListPage(ctx, 10, 10, conds)
	if err != nil {
		t.Fatalf("ListPage: %v", err)
	}
	if hasMore || len(page2) != 0 {
		t.Fatalf("第二页 hasMore=%v len=%d, 期望 false/0", hasMore, len(page2))
	}

	// 4. 再造 3 条，验证 has_more=true 的翻页路径
	for i := 0; i < 3; i++ {
		mkTask("repo-functional-task-" + string(rune('a'+i)))
	}
	_, total2, err := r.List(ctx, 0, 10, conds)
	if err != nil {
		t.Fatal(err)
	}
	if total2 != 4 {
		t.Fatalf("total = %d, 期望 4", total2)
	}
	page1, hasMore1, err := r.ListPage(ctx, 0, 2, conds)
	if err != nil {
		t.Fatal(err)
	}
	if len(page1) != 2 || !hasMore1 {
		t.Fatalf("第一页 len=%d hasMore=%v, 期望 2/true（多取一条的翻页判定）", len(page1), hasMore1)
	}
	// 第三页越界：空且无更多
	_, hasMore3, err := r.ListPage(ctx, 4, 2, conds)
	if err != nil {
		t.Fatal(err)
	}
	if hasMore3 {
		t.Fatal("越界页 hasMore 应当为 false")
	}
}
