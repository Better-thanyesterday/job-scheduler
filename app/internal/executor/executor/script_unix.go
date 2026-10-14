//go:build !windows

package executor

import (
	"os"
	"syscall"
)

// processGroupSysProcAttr 把脚本进程放进独立进程组：
// bash -c 起的子进程（sleep/管道等）和主进程同组，超时清理时按组整体 kill，
// 否则子进程占着 stdout 管道会让 Run 一直阻塞
func processGroupSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true}
}

func killProcessTree(p *os.Process) error {
	if p == nil {
		return nil
	}
	// 负 pid 表示整个进程组
	return syscall.Kill(-p.Pid, syscall.SIGKILL)
}
