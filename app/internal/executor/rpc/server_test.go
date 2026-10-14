package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"job-scheduler/app/internal/domain/coordination"
	"job-scheduler/app/internal/domain/execution"
	"job-scheduler/app/internal/executor/executor"
	"job-scheduler/app/internal/executor/handler"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// fakeRepo 内存版 ExecutorRepo：模拟 ClaimPending 原子抢票 + 终态落库
type fakeRepo struct {
	mu    sync.Mutex
	rows  map[int64]*execution.Execution
	claim func() error // 可注入基础设施故障
}

func newFakeRepo() *fakeRepo { return &fakeRepo{rows: make(map[int64]*execution.Execution)} }

func (f *fakeRepo) register(id int64) {
	e, _ := execution.Rebuild(id, 100, 0, 0, "", execution.StatusPending, 0,
		time.Time{}, time.Time{}, 0, nil, "", 0, 3, "", "trace-1", nil, time.Now())
	f.rows[id] = e
}

func (f *fakeRepo) ClaimPending(ctx context.Context, id int64, workerID string, now time.Time) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.claim != nil {
		if err := f.claim(); err != nil {
			return false, err
		}
	}
	row, ok := f.rows[id]
	if !ok {
		return false, nil
	}
	if row.Status() != execution.StatusPending && row.Status() != execution.StatusScheduled {
		return false, nil
	}
	if err := row.Claim(workerID, now); err != nil {
		return false, nil
	}
	return true, nil
}

func (f *fakeRepo) UpdateTerminal(ctx context.Context, e *execution.Execution) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	row, ok := f.rows[e.ID()]
	if !ok || row.Status() != execution.StatusRunning {
		return fmt.Errorf("%w: id=%d", execution.ErrWrongStatus, e.ID())
	}
	f.rows[e.ID()] = e
	return nil
}

func (f *fakeRepo) row(id int64) *execution.Execution {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rows[id]
}

// stubExecutor 固定返回成功/失败
type stubExecutor struct{ err error }

func (s *stubExecutor) Execute(ctx context.Context, msg *coordination.ExecutionMessage) ([]byte, error) {
	if s.err != nil {
		return nil, s.err
	}
	return []byte(`{"ok":true}`), nil
}

// fakeNotifier 记录结果回执的假通知器（非线程安全：测试串行推送）
type fakeNotifier struct {
	results []*coordination.ExecutionResult
}

func (f *fakeNotifier) NotifyResult(ctx context.Context, r *coordination.ExecutionResult) {
	f.results = append(f.results, r)
}

// harness 起 bufconn gRPC server + client
type harness struct {
	repo     *fakeRepo
	client   ExecutorServiceClient
	notifier *fakeNotifier
	close    func()
}

func start(t *testing.T, ex executor.Executor) *harness {
	return startWithNotifier(t, ex, nil)
}

func startWithNotifier(t *testing.T, ex executor.Executor, notifier *fakeNotifier) *harness {
	t.Helper()
	repo := newFakeRepo()
	reg := executor.Registry{1: ex}
	// nil 的 *fakeNotifier 装进接口会变成"非 nil 接口持有 nil 指针"，
	// handler 的判空会被骗过——这里显式归一成 nil 接口
	var n handler.ResultNotifier
	if notifier != nil {
		n = notifier
	}
	h := handler.NewHandler(repo, reg, "test-worker", n)

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	RegisterExecutorServiceServer(srv, NewServer(h))
	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc client: %v", err)
	}
	return &harness{
		repo:     repo,
		client:   NewExecutorServiceClient(conn),
		notifier: notifier,
		close: func() {
			_ = conn.Close()
			srv.Stop()
		},
	}
}

func validMsg(id int64) *coordination.ExecutionMessage {
	return &coordination.ExecutionMessage{
		ExecID: id, TaskID: 100, TaskType: 1, Config: json.RawMessage(`{}`),
		Priority: 5, TimeoutSec: 5, EnqueuedAt: time.Now(), TraceID: "trace-1",
	}
}

func push(t *testing.T, c ExecutorServiceClient, msg *coordination.ExecutionMessage) (*ExecuteResponse, error) {
	t.Helper()
	body, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return c.Execute(ctx, &ExecuteRequest{Payload: body})
}

func TestExecuteSuccessAndIdempotentDup(t *testing.T) {
	h := start(t, &stubExecutor{})
	defer h.close()

	h.repo.register(1)
	resp, err := push(t, h.client, validMsg(1))
	if err != nil {
		t.Fatalf("first push: %v", err)
	}
	if !resp.GetClaimed() || !resp.GetSuccess() || resp.GetErrorCode() != int32(ErrorCode_OK) {
		t.Fatalf("first push unexpected resp: %+v", resp)
	}
	if row := h.repo.row(1); row.Status() != execution.StatusSuccess {
		t.Fatalf("row status = %v, want Success", row.Status())
	}

	// 同一 ExecID 再推：ClaimPending 幂等丢弃，claimed=false
	resp2, err := push(t, h.client, validMsg(1))
	if err != nil {
		t.Fatalf("dup push: %v", err)
	}
	if resp2.GetClaimed() || resp2.GetErrorCode() != int32(ErrorCode_CLAIMED_FALSE) {
		t.Fatalf("dup push unexpected resp: %+v", resp2)
	}
}

func TestExecuteFailed(t *testing.T) {
	h := start(t, &stubExecutor{err: errors.New("boom")})
	defer h.close()

	h.repo.register(2)
	resp, err := push(t, h.client, validMsg(2))
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	if !resp.GetClaimed() || resp.GetSuccess() || resp.GetErrorCode() != int32(ErrorCode_EXEC_FAILED) {
		t.Fatalf("unexpected resp: %+v", resp)
	}
	if row := h.repo.row(2); row.Status() != execution.StatusFailed {
		t.Fatalf("row status = %v, want Failed", row.Status())
	}
}

func TestInfraErrorMapsToUnavailable(t *testing.T) {
	h := start(t, &stubExecutor{})
	defer h.close()

	h.repo.register(3)
	h.repo.claim = func() error { return errors.New("db down") }
	_, err := push(t, h.client, validMsg(3))
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("code = %v, want Unavailable", status.Code(err))
	}
}

func TestInvalidPayloadMapsToInvalidArgument(t *testing.T) {
	h := start(t, &stubExecutor{})
	defer h.close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := h.client.Execute(ctx, &ExecuteRequest{Payload: []byte("{not json")})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument", status.Code(err))
	}
}

// dagMsg 带 DAG 来源字段的合法消息
func dagMsg(id int64) *coordination.ExecutionMessage {
	m := validMsg(id)
	m.DagExecID = 777
	m.DagNodeID = "extract"
	return m
}

func TestResultNotification(t *testing.T) {
	t.Run("cron 成功 → cron.success 回执", func(t *testing.T) {
		notifier := &fakeNotifier{}
		h := startWithNotifier(t, &stubExecutor{}, notifier)
		defer h.close()

		h.repo.register(11)
		if _, err := push(t, h.client, validMsg(11)); err != nil {
			t.Fatalf("push: %v", err)
		}
		if len(notifier.results) != 1 {
			t.Fatalf("want 1 notification, got %d", len(notifier.results))
		}
		r := notifier.results[0]
		if !r.Success || r.FromDag() || r.ErrorMsg != "" {
			t.Fatalf("unexpected result: %+v", r)
		}
		if r.WorkerID != "test-worker" || r.ExecID != 11 || r.DurationMS < 0 {
			t.Fatalf("fields wrong: %+v", r)
		}
	})

	t.Run("dag 失败 → dag.failed 回执", func(t *testing.T) {
		notifier := &fakeNotifier{}
		h := startWithNotifier(t, &stubExecutor{err: errors.New("boom")}, notifier)
		defer h.close()

		h.repo.register(12)
		if _, err := push(t, h.client, dagMsg(12)); err != nil {
			t.Fatalf("push: %v", err)
		}
		r := notifier.results[0]
		if r.Success || !r.FromDag() || r.ErrorMsg != "boom" {
			t.Fatalf("unexpected result: %+v", r)
		}
		if r.FailureClass != coordination.FailureClassExecution {
			t.Fatalf("failure class = %q, want execution", r.FailureClass)
		}
		if r.DagExecID != 777 || r.DagNodeID != "extract" {
			t.Fatalf("dag fields wrong: %+v", r)
		}
	})

	t.Run("执行超时 → timeout 分类回执", func(t *testing.T) {
		notifier := &fakeNotifier{}
		h := startWithNotifier(t, &stubExecutor{err: context.DeadlineExceeded}, notifier)
		defer h.close()

		h.repo.register(15)
		if _, err := push(t, h.client, validMsg(15)); err != nil {
			t.Fatalf("push: %v", err)
		}
		r := notifier.results[0]
		if r.Success || r.FromDag() {
			t.Fatalf("unexpected result: %+v", r)
		}
		if r.FailureClass != coordination.FailureClassTimeout {
			t.Fatalf("failure class = %q, want timeout", r.FailureClass)
		}
	})

	t.Run("重复投递幂等丢弃不再通知", func(t *testing.T) {
		notifier := &fakeNotifier{}
		h := startWithNotifier(t, &stubExecutor{}, notifier)
		defer h.close()

		h.repo.register(13)
		_, _ = push(t, h.client, validMsg(13))
		_, _ = push(t, h.client, validMsg(13)) // 幂等丢弃
		if len(notifier.results) != 1 {
			t.Fatalf("dup push should not notify again, got %d", len(notifier.results))
		}
	})

	t.Run("notifier 为 nil 时静默跳过", func(t *testing.T) {
		h := start(t, &stubExecutor{}) // 不注入 notifier
		defer h.close()

		h.repo.register(14)
		resp, err := push(t, h.client, validMsg(14))
		if err != nil || !resp.GetSuccess() {
			t.Fatalf("push should succeed without notifier: %+v %v", resp, err)
		}
	})
}
