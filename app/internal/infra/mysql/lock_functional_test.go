package mysql_test

import (
	"context"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"

	"job-scheduler/app/internal/domain/coordination"
	"job-scheduler/app/internal/infra/mysql"
)

const testDSN = "root:root@tcp(127.0.0.1:3306)/job_task?parseTime=true&loc=Local"

// 功能验证：GET_LOCK 分布式锁——互斥获取、续期、幂等释放、释放后可再抢
func TestLockManagerFunctional(t *testing.T) {
	db, err := sqlx.Connect("mysql", testDSN)
	if err != nil {
		t.Skipf("MySQL 不可用，跳过真库功能测试: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	m1 := mysql.NewLockManager(db)
	m2 := mysql.NewLockManager(db)

	// 1. 首抢成功
	h1, err := m1.Acquire(ctx, "functional-test:lock", time.Minute)
	if err != nil {
		t.Fatalf("首次获取锁应当成功: %v", err)
	}

	// 2. 互斥：另一个 manager 抢同一把锁必须失败
	if _, err := m2.Acquire(ctx, "functional-test:lock", time.Minute); err == nil {
		t.Fatal("锁被 m1 持有时，m2 不应当抢到")
	} else if err != coordination.ErrLockNotAcquired {
		t.Fatalf("抢锁失败应当返回 ErrLockNotAcquired, 实际: %v", err)
	}

	// 3. 续期：持有人续期成功
	if err := m1.Refresh(ctx, h1, time.Minute); err != nil {
		t.Fatalf("持有人续期应当成功: %v", err)
	}

	// 4. 释放 + 幂等释放
	if err := m1.Release(ctx, h1); err != nil {
		t.Fatalf("持有人释放应当成功: %v", err)
	}
	if err := m1.Release(ctx, h1); err != nil {
		t.Fatalf("重复释放应当幂等返回 nil: %v", err)
	}

	// 5. 释放后他人可抢，再清理干净
	h2, err := m2.Acquire(ctx, "functional-test:lock", time.Minute)
	if err != nil {
		t.Fatalf("释放后 m2 应当能抢到: %v", err)
	}
	if err := m2.Release(ctx, h2); err != nil {
		t.Fatalf("m2 释放应当成功: %v", err)
	}
}
