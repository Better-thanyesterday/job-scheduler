package types

// ---------- DAG YAML 导入契约 ----------
// 前端把整个 YAML 文本作为请求体 POST 上来（Content-Type 建议 text/yaml），
// controller 用 gin 的 YAML binding 反序列化到 DagYAML，logic 层转领域实体落库。
//
// 示例：
//
//	name: data-pipeline
//	description: 每日数据管道
//	parallel_limit: 3
//	fail_fast: true
//	nodes:
//	  - id: extract
//	    task_id: 1001
//	    name: 抽取
//	  - id: transform
//	    task_id: 1002
//	    name: 清洗
//	edges:
//	  - from: extract
//	    to: transform
//	    type: success   # success | failure | always，缺省 success

// DagYAML DAG 定义的 YAML 形态。校验分两层：binding 标签管"长得像不像"，
// 语义不变量（nodeID 唯一、边引用完整、无环）由领域 dag.New 把关
type DagYAML struct {
	Name          string        `yaml:"name" binding:"required"`
	Description   string        `yaml:"description"`
	ParallelLimit int32         `yaml:"parallel_limit"` // 0 = 不限并发
	FailFast      *bool         `yaml:"fail_fast"`      // 缺省 true（与 dags 表默认值一致）
	Nodes         []DagNodeYAML `yaml:"nodes" binding:"required,min=1,dive"`
	Edges         []DagEdgeYAML `yaml:"edges" binding:"omitempty,dive"`
}

type DagNodeYAML struct {
	ID     string `yaml:"id" binding:"required" json:"id"` // DAG 内唯一标识，edges 靠它连线
	TaskID int64  `yaml:"task_id" binding:"required" json:"task_id"`
	Name   string `yaml:"name" json:"name"`
}

type DagEdgeYAML struct {
	From string `yaml:"from" binding:"required" json:"from"`
	To   string `yaml:"to" binding:"required" json:"to"`
	Type string `yaml:"type" json:"type"` // success | failure | always，缺省 success
}

// ---------- DAG 响应 ----------

type DagResp struct {
	ID            int64         `json:"id"`
	Name          string        `json:"name"`
	Description   string        `json:"description"`
	ParallelLimit int32         `json:"parallel_limit"`
	FailFast      bool          `json:"fail_fast"`
	Status        int           `json:"status"` // 1-启用 2-禁用 3-删除
	CreatedBy     int64         `json:"created_by"`
	CreatedAt     string        `json:"created_at"`
	UpdatedAt     string        `json:"updated_at"`
	Nodes         []DagNodeResp `json:"nodes"`
	Edges         []DagEdgeResp `json:"edges"`
}

type DagNodeResp struct {
	NodeID string `json:"node_id"`
	TaskID int64  `json:"task_id"`
	Name   string `json:"name"`
}

type DagEdgeResp struct {
	FromNodeID string `json:"from_node_id"`
	ToNodeID   string `json:"to_node_id"`
	EdgeType   int    `json:"edge_type"` // 1-成功依赖 2-失败依赖 3-总是执行
}

// DagRunResp 发起运行的回执：运行记录已落库，dag 引擎会推进它
type DagRunResp struct {
	ID     int64 `json:"id"` // dag_execution_id
	DagID  int64 `json:"dag_id"`
	Status int   `json:"status"` // 1-Running（出生即运行中）
}
