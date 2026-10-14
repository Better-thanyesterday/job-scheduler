package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"job-scheduler/app/internal/domain/coordination"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
)

const keyPrefix = "job:lock:" // 物理命名空间：避免与同库其他应用的 GET_LOCK 撞名
// （业务前缀 "schedule:" 由调用方拼，两层各管各的）

// LockManager LockManager 的 MySQL GET_LOCK 实现。
//
// ★ DSN 纪律：m.db 必须直连主库，禁止经过读写分离代理——
//   代理可能把"同一次锁操作"拆到不同后端连接上，GET_LOCK 的
//   连接级语义直接作废。装配时给锁专用 DSN（可复用主库，不可复用
//   带代理的连接池）。
//
// ★ 连接纪律：每把锁独占一条 *sql.Conn，持有期间不回池。
//   依赖 MySQL ≥ 5.7.5 更佳（同连接可持多把命名锁），但由于我们
//   一锁一连接，5.6 也能工作。

type LockManager struct {
	db   *sqlx.DB
	mu   sync.Mutex
	held map[string]*lockState // key → 持有状态（进程内所有权账本）
}

type lockState struct {
	conn     *sql.Conn
	connID   uint64 // CONNECTION_ID()：所有权凭证
	fullKey  string
	timer    *time.Timer // TTL 看门狗
	released bool        // 幂等释放标记
}

func NewLockManager(db *sqlx.DB) *LockManager {
	return &LockManager{db: db, held: make(map[string]*lockState)}
}

// Acquire 非阻塞获取命名锁（接口契约：拿不到立即 ErrLockNotAcquired）。
func (m *LockManager) Acquire(ctx context.Context, key string, ttl time.Duration) (*coordination.LockHandle, error) {
	full := keyPrefix + key

	// ① 专用连接（决定 1）
	conn, err := m.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("lock: get conn: %w", err)
	}

	// 记下自己的连接 ID（所有权凭证）
	var connID uint64
	if err := conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&connID); err != nil {
		closeQuietly(conn, full) // 见下方 closeQuietly：防御性释放后归还
		return nil, fmt.Errorf("lock: connection id: %w", err)
	}

	// ② 非阻塞抢锁：GET_LOCK(name, 0)
	//    返回 1=拿到 / 0=被占 / NULL=出错（如内存不足）
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", full).Scan(&got); err != nil {
		closeQuietly(conn, full)
		return nil, fmt.Errorf("lock: get_lock: %w", err)
	}
	if !got.Valid || got.Int64 != 1 {
		closeQuietly(conn, full) // 没抢到：连接归还池子（未持锁，直接还）
		return nil, coordination.ErrLockNotAcquired
	}

	// ③ 建句柄 + 登记所有权账本 + 启动 TTL 看门狗（决定 2）
	expiresAt := time.Now().Add(ttl)
	h := coordination.NewLockHandle(key, fmt.Sprintf("conn:%d", connID), expiresAt)
	st := &lockState{conn: conn, connID: connID, fullKey: full}
	st.timer = time.AfterFunc(ttl, func() { m.expire(key) }) // TTL 到点自动释放

	m.mu.Lock()
	// 理论上同 key 不会到这里两次（服务端已互斥），防御性兜底
	if old, ok := m.held[key]; ok {
		m.mu.Unlock()
		old.releaseQuietly()
	} else {
		m.held[key] = st
		m.mu.Unlock()
	}
	return h, nil
}

// Release 主动释放。幂等：重复释放返回 nil；非本人持有返回 error。
func (m *LockManager) Release(ctx context.Context, h *coordination.LockHandle) error {
	st := m.take(h.Key())
	if st == nil {
		return nil // 从未持有 / 已释放 / 已被看门狗收走 → 幂等
	}
	defer st.cleanup() // 停看门狗 + 连接归还池

	// 所有权校验（决定 3）：IS_USED_LOCK 返回持锁者的 CONNECTION_ID，
	// 未被持有返回 NULL
	var holder sql.NullInt64
	if err := st.conn.QueryRowContext(ctx,
		"SELECT IS_USED_LOCK(?)", st.fullKey).Scan(&holder); err != nil {
		return fmt.Errorf("lock: verify owner: %w", err)
	}
	if !holder.Valid || uint64(holder.Int64) != st.connID {
		return coordination.ErrLockLost // 锁已易主/已消失，不能替别人放
	}

	var res sql.NullInt64
	if err := st.conn.QueryRowContext(ctx,
		"SELECT RELEASE_LOCK(?)", st.fullKey).Scan(&res); err != nil {
		return fmt.Errorf("lock: release: %w", err)
	}
	// 1=释放成功 / 0=非持有者 / NULL=锁不存在 → 后两种按幂等处理返回 nil
	return nil
}

// Refresh 续期：校验仍是本人后重置看门狗。锁已易主返回 ErrLockLost。
// （GET_LOCK 无原生续期——TTL 是我们客户端实现的，所以"续期"就是重置定时器）
func (m *LockManager) Refresh(ctx context.Context, h *coordination.LockHandle, ttl time.Duration) error {
	m.mu.Lock()
	st, ok := m.held[h.Key()]
	m.mu.Unlock()
	if !ok || st.released {
		return coordination.ErrLockLost
	}

	var holder sql.NullInt64
	if err := st.conn.QueryRowContext(ctx,
		"SELECT IS_USED_LOCK(?)", st.fullKey).Scan(&holder); err != nil {
		return fmt.Errorf("lock: refresh verify: %w", err)
	}
	if !holder.Valid || uint64(holder.Int64) != st.connID {
		m.take(h.Key()) // 从账本摘除
		st.cleanup()
		return coordination.ErrLockLost
	}
	if !st.timer.Stop() {
		select {
		case <-st.timer.C:
		default:
		}
	}
	st.timer.Reset(ttl)
	return nil
}

// ---- 内部辅助 ----

// expire 看门狗到点：TTL 已过，自动释放（接口承诺"ttl 到期自动释放"的落点）
func (m *LockManager) expire(key string) {
	if st := m.take(key); st != nil {
		st.releaseQuietly()
		st.cleanup()
	}
}

// take 从账本摘除并返回持有状态（并发下保证只被处理一次）
func (m *LockManager) take(key string) *lockState {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.held[key]
	delete(m.held, key)
	return st
}

func (s *lockState) cleanup() {
	s.timer.Stop()
	if s.conn != nil {
		s.conn.Close() // 归还连接池
	}
}

// releaseQuietly 尽力而为的释放：用于错误路径/看门狗路径，
// 确保连接回池前锁已解——否则池里会留下"带锁连接"污染后续复用
func (s *lockState) releaseQuietly() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _ = s.conn.ExecContext(ctx, "SELECT RELEASE_LOCK(?)", s.fullKey)
}

// closeQuietly Acquire 失败路径用：未持锁成功的连接直接归还即可；
// 但 GET_LOCK 已成功而后续步骤失败的路径必须先 releaseQuietly
func closeQuietly(conn *sql.Conn, fullKey string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _ = conn.ExecContext(ctx, "SELECT RELEASE_LOCK(?)", fullKey) // 未持锁时无害（返回 NULL）
	conn.Close()
}
