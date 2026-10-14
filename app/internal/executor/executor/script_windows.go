//go:build windows

package executor

import (
	"os"
	"os/exec"
	"strconv"
	"syscall"
)

// processGroupSysProcAttr 新开一个进程组：taskkill /T 按树清理时以组为单位
func processGroupSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP}
}

func killProcessTree(p *os.Process) error {
	if p == nil {
		return nil
	}
	// /T 连同子进程树一起杀，/F 强制
	return exec.Command("taskkill", "/T", "/F", "/PID", strconv.Itoa(p.Pid)).Run()
}
