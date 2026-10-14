package dag

import (
	"fmt"
	"job-scheduler/app/pkg/snowflake"
	"time"
)

type Dag struct {
	id            int64
	name          string
	description   string
	parallelLimit int32
	failFast      bool
	status        DefStatus // dags.status:1-启用 2-禁用 3-删除
	createdBy     int64
	createdAt     time.Time
	updatedAt     time.Time
	nodes         []*DagNode
	edges         []Edge // 按 nodeID 引用,不持指针
}

// New 定义态构造:出生即校验(nodeID 唯一、边引用完整、无环)
func New(name, description string, parallelLimit int32, failFast bool, createdBy int64,
	nodes []*DagNode, edges []Edge) (*Dag, error) {
	now := time.Now()
	d := &Dag{
		id:            snowflake.GenID(),
		name:          name,
		description:   description,
		parallelLimit: parallelLimit,
		failFast:      failFast,
		status:        DefEnabled,
		createdBy:     createdBy,
		createdAt:     now,
		updatedAt:     now,
		nodes:         nodes,
		edges:         edges,
	}
	if err := d.Validate(); err != nil {
		return nil, err
	}
	return d, nil
}

// Rebuild 从持久化重建。落库后的图仍可能被改坏,重建时重新过一遍校验
func Rebuild(id int64, name, description string, parallelLimit int32, failFast bool,
	status DefStatus, createdBy int64, createdAt, updatedAt time.Time,
	nodes []*DagNode, edges []Edge) (*Dag, error) {
	if !status.Valid() {
		return nil, fmt.Errorf("dag: rebuild id=%d invalid status %d", id, status)
	}
	d := &Dag{
		id:            id,
		name:          name,
		description:   description,
		parallelLimit: parallelLimit,
		failFast:      failFast,
		status:        status,
		createdBy:     createdBy,
		createdAt:     createdAt,
		updatedAt:     updatedAt,
		nodes:         nodes,
		edges:         edges,
	}
	if err := d.Validate(); err != nil {
		return nil, err
	}
	return d, nil
}

// Validate 校验定义不变量:
//  1. 名称非空(dags.name 非空,空名在列表页无法辨认)
//  2. 至少一个节点,nodeID 非空且 DAG 内唯一
//  3. 每条边的两端都指向已定义的节点
//  4. 依赖图无环(含自环)
func (d *Dag) Validate() error {
	if d.name == "" {
		return fmt.Errorf("%w: name is empty", ErrInvalidDefinition)
	}
	if d.parallelLimit < 0 {
		return fmt.Errorf("%w: negative parallel_limit %d", ErrInvalidDefinition, d.parallelLimit)
	}
	if len(d.nodes) == 0 {
		return fmt.Errorf("%w: no nodes", ErrInvalidDefinition)
	}
	index := make(map[string]bool, len(d.nodes))
	for _, n := range d.nodes {
		if n.nodeID == "" {
			return fmt.Errorf("%w: empty node_id", ErrInvalidDefinition)
		}
		if index[n.nodeID] {
			return fmt.Errorf("%w: duplicate node_id %q", ErrInvalidDefinition, n.nodeID)
		}
		index[n.nodeID] = true
	}
	for _, e := range d.edges {
		if !index[e.fromNodeID] {
			return fmt.Errorf("%w: edge from unknown node %q", ErrInvalidDefinition, e.fromNodeID)
		}
		if !index[e.toNodeID] {
			return fmt.Errorf("%w: edge to unknown node %q", ErrInvalidDefinition, e.toNodeID)
		}
	}
	if err := d.checkAcyclic(); err != nil {
		return err
	}
	return nil
}

// checkAcyclic Kahn 拓扑:所有边(不分类型)都构成先后约束
func (d *Dag) checkAcyclic() error {
	indegree := make(map[string]int, len(d.nodes))
	adjacency := make(map[string][]string, len(d.nodes))
	for _, n := range d.nodes {
		indegree[n.nodeID] = 0
	}
	for _, e := range d.edges {
		adjacency[e.fromNodeID] = append(adjacency[e.fromNodeID], e.toNodeID)
		indegree[e.toNodeID]++
	}
	queue := make([]string, 0, len(d.nodes))
	for _, n := range d.nodes {
		if indegree[n.nodeID] == 0 {
			queue = append(queue, n.nodeID)
		}
	}
	processed := 0
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		processed++
		for _, next := range adjacency[cur] {
			indegree[next]--
			if indegree[next] == 0 {
				queue = append(queue, next)
			}
		}
	}
	if processed != len(d.nodes) {
		stuck := make([]string, 0)
		for _, n := range d.nodes {
			if indegree[n.nodeID] > 0 {
				stuck = append(stuck, n.nodeID)
			}
		}
		return fmt.Errorf("%w: nodes %v", ErrCycle, stuck)
	}
	return nil
}

// Node 取节点定义;不存在返回 nil
func (d *Dag) Node(nodeID string) *DagNode {
	for _, n := range d.nodes {
		if n.nodeID == nodeID {
			return n
		}
	}
	return nil
}

func (d *Dag) Nodes() []*DagNode { return d.nodes }
func (d *Dag) Edges() []Edge     { return d.edges }

// Incoming 指向 nodeID 的所有入边(依赖判定用)
func (d *Dag) Incoming(nodeID string) []Edge {
	in := make([]Edge, 0)
	for _, e := range d.edges {
		if e.toNodeID == nodeID {
			in = append(in, e)
		}
	}
	return in
}

// TopoOrder Kahn 序(同层保持定义顺序),引擎派发/展示用
func (d *Dag) TopoOrder() []*DagNode {
	indegree := make(map[string]int, len(d.nodes))
	adjacency := make(map[string][]string, len(d.nodes))
	for _, n := range d.nodes {
		indegree[n.nodeID] = 0
	}
	for _, e := range d.edges {
		adjacency[e.fromNodeID] = append(adjacency[e.fromNodeID], e.toNodeID)
		indegree[e.toNodeID]++
	}
	queue := make([]string, 0, len(d.nodes))
	for _, n := range d.nodes {
		if indegree[n.nodeID] == 0 {
			queue = append(queue, n.nodeID)
		}
	}
	order := make([]*DagNode, 0, len(d.nodes))
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if n := d.Node(cur); n != nil {
			order = append(order, n)
		}
		for _, next := range adjacency[cur] {
			indegree[next]--
			if indegree[next] == 0 {
				queue = append(queue, next)
			}
		}
	}
	return order
}

func (d *Dag) ID() int64            { return d.id }
func (d *Dag) Name() string         { return d.name }
func (d *Dag) Description() string  { return d.description }
func (d *Dag) ParallelLimit() int32 { return d.parallelLimit }
func (d *Dag) FailFast() bool       { return d.failFast }
func (d *Dag) Status() DefStatus    { return d.status }
func (d *Dag) CreatedBy() int64     { return d.createdBy }
func (d *Dag) CreatedAt() time.Time { return d.createdAt }
func (d *Dag) UpdatedAt() time.Time { return d.updatedAt }

// Active 是否可接受新的运行(引擎触发前的判定)
func (d *Dag) Active() bool { return d.status == DefEnabled }

// ---------- 定义态行为(api 的启停删按钮)----------

// Disable 停用:定义保留,不再接受新运行
func (d *Dag) Disable(now time.Time) error {
	if d.status == DefDeleted {
		return fmt.Errorf("%w: dag %d is deleted", ErrWrongStatus, d.id)
	}
	d.status = DefDisabled
	d.updatedAt = now
	return nil
}

// Enable 启用
func (d *Dag) Enable(now time.Time) error {
	if d.status == DefDeleted {
		return fmt.Errorf("%w: dag %d is deleted", ErrWrongStatus, d.id)
	}
	d.status = DefEnabled
	d.updatedAt = now
	return nil
}

// MarkDeleted 软删:deleted_at 由 infra 一并写入
func (d *Dag) MarkDeleted(now time.Time) error {
	if d.status == DefDeleted {
		return fmt.Errorf("%w: dag %d already deleted", ErrWrongStatus, d.id)
	}
	d.status = DefDeleted
	d.updatedAt = now
	return nil
}
