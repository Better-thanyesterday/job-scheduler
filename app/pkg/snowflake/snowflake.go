package snowflake

import (
	"fmt"
	"sync"
	"time"

	"github.com/bwmarrin/snowflake"
)

var node *snowflake.Node
var once sync.Once

func Init(startTime string, machineID int64) (err error) {
	var st time.Time
	st, err = time.Parse("2006-01-02", startTime)
	if err != nil {
		return fmt.Errorf("time parse failed:%v", err)
	}
	snowflake.Epoch = st.UnixNano() / 1000000
	node, err = snowflake.NewNode(machineID)
	return err
}

// GenID 未 Init 时兜底生成一个默认节点,保证单测等
// 未走服务启动流程的场景不至于空指针 panic
func GenID() int64 {
	if node == nil {
		once.Do(func() {
			if node == nil {
				n, err := snowflake.NewNode(1)
				if err != nil {
					panic(err)
				}
				node = n
			}
		})
	}
	return node.Generate().Int64()
}
