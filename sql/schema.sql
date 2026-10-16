-- job-scheduler 建表脚本（五库分域：job_task/job_schedule/job_execution/job_dag/job_system）
-- 表归属：job_task=tasks；job_schedule=schedules,schedule_histories；job_execution=executions,execution_logs；
--        job_dag=dags,dag_nodes,dag_edges,dag_executions,dag_node_executions；job_system=users
-- 由开发库 mysqldump 导出（仅结构）。用法：先创建各库，再在对应库执行相应表段。

CREATE TABLE `tasks` (
  `id` bigint NOT NULL COMMENT '任务ID（雪花算法）',
  `name` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '任务名称',
  `task_type` tinyint NOT NULL COMMENT '任务类型：1-HTTP 2-脚本 3-消息 4-SQL 5-子工作流',
  `config` json NOT NULL COMMENT '任务配置体',
  `priority` tinyint NOT NULL DEFAULT '5' COMMENT '优先级 1-10',
  `timeout` int NOT NULL DEFAULT '300' COMMENT '超时时间（秒）',
  `retry_policy` json DEFAULT NULL COMMENT '重试策略',
  `status` tinyint NOT NULL DEFAULT '1' COMMENT '状态：1-启用 2-禁用 3-删除',
  `description` varchar(512) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '任务描述',
  `created_by` bigint NOT NULL COMMENT '创建者用户ID',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  `deleted_at` datetime(3) DEFAULT NULL COMMENT '软删除时间',
  PRIMARY KEY (`id`),
  KEY `idx_name` (`name`),
  KEY `idx_createdby_deleted_status_type_createtime` (`created_by`,`deleted_at`,`task_type`,`status`,`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `schedule_histories` (
  `id` bigint NOT NULL,
  `schedule_id` bigint NOT NULL,
  `execution_id` bigint DEFAULT NULL COMMENT '关联执行记录ID',
  `trigger_time` datetime(3) NOT NULL,
  `planned_time` datetime(3) NOT NULL,
  `lag_ms` int NOT NULL DEFAULT '0',
  `status` tinyint NOT NULL DEFAULT '1' COMMENT '1-成功 2-失败 3-跳过',
  `triggered_by` tinyint NOT NULL DEFAULT '1' COMMENT '1-Cron 2-手动 3-补偿',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  KEY `idx_schedule_histories_schedule` (`schedule_id`,`trigger_time` DESC),
  KEY `idx_schedule_histories_lag` (`status`,`lag_ms`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `schedules` (
  `id` bigint NOT NULL,
  `task_id` bigint NOT NULL COMMENT '触发目标为任务时的任务ID（target_type=1）',
  `target_type` tinyint NOT NULL DEFAULT '1' COMMENT '触发目标：1-任务 2-DAG',
  `dag_id` bigint NOT NULL DEFAULT '0' COMMENT '触发目标为DAG时的DAG ID（target_type=2）',
  `name` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
  `cron_expr` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL,
  `schedule_type` tinyint NOT NULL DEFAULT '1' COMMENT '1-Cron 2-手动 3-API触发',
  `next_run_time` datetime(3) DEFAULT NULL COMMENT '下次执行时间',
  `last_run_time` datetime(3) DEFAULT NULL COMMENT '上次执行时间',
  `status` tinyint NOT NULL DEFAULT '1' COMMENT '1-运行中 2-暂停 3-过期 4-删除',
  `misfire_policy` tinyint NOT NULL DEFAULT '1' COMMENT '1-立即补发 2-跳过 3-合并',
  `timezone` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'Asia/Shanghai',
  `created_by` bigint NOT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  `deleted_at` datetime(3) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_schedules_status_next_run` (`status`,`next_run_time`),
  KEY `idx_schedules_task_id` (`task_id`,`status`),
  KEY `idx_schedules_dag_id` (`dag_id`,`status`),
  KEY `idx_schedules_created_by` (`created_by`,`status`,`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `execution_logs` (
  `id` bigint NOT NULL,
  `execution_id` bigint NOT NULL,
  `task_id` bigint NOT NULL,
  `log_type` tinyint NOT NULL DEFAULT '1' COMMENT '1-stdout 2-stderr 3-系统日志',
  `log_content` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `seq` int NOT NULL DEFAULT '0',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  KEY `idx_execution_logs_execution` (`execution_id`,`seq`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `executions` (
  `id` bigint NOT NULL,
  `task_id` bigint NOT NULL,
  `schedule_id` bigint DEFAULT NULL,
  `dag_execution_id` bigint DEFAULT NULL,
  `dag_node_id` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `status` tinyint NOT NULL DEFAULT '1' COMMENT '1-Pending 2-Scheduled 3-Running 4-Success 5-Failed 6-Retrying 7-Failed_final',
  `start_time` datetime(3) DEFAULT NULL,
  `end_time` datetime(3) DEFAULT NULL,
  `duration_ms` int DEFAULT NULL,
  `result` json DEFAULT NULL,
  `error_msg` text COLLATE utf8mb4_unicode_ci,
  `retry_count` int NOT NULL DEFAULT '0',
  `max_retry` int NOT NULL DEFAULT '3',
  `worker_id` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `trace_id` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  `version` int DEFAULT NULL,
  `next_retry_at` datetime(3) DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  KEY `idx_executions_task_created` (`task_id`,`created_at` DESC),
  KEY `idx_executions_status_created` (`status`,`created_at` DESC),
  KEY `idx_executions_dag` (`dag_execution_id`,`status`),
  KEY `idx_executions_schedule` (`schedule_id`,`created_at` DESC),
  KEY `idx_executions_trace` (`trace_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `dag_edges` (
  `id` bigint NOT NULL,
  `dag_id` bigint NOT NULL,
  `from_node_id` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL,
  `to_node_id` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL,
  `edge_type` tinyint NOT NULL DEFAULT '1' COMMENT '1-成功依赖 2-失败依赖 3-总是执行',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  KEY `idx_dag_edges_dag_from` (`dag_id`,`from_node_id`),
  KEY `idx_dag_edges_dag_to` (`dag_id`,`to_node_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `dag_executions` (
  `id` bigint NOT NULL,
  `dag_id` bigint NOT NULL,
  `status` tinyint NOT NULL DEFAULT '1' COMMENT '1-运行中 2-成功 3-失败 4-部分成功',
  `start_time` datetime(3) DEFAULT NULL,
  `end_time` datetime(3) DEFAULT NULL,
  `duration_ms` int DEFAULT NULL,
  `context` json DEFAULT NULL COMMENT '执行上下文',
  `triggered_by` tinyint NOT NULL DEFAULT '1' COMMENT '1-Cron 2-手动 3-子工作流',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  KEY `idx_dag_executions_dag` (`dag_id`,`created_at` DESC),
  KEY `idx_dag_executions_status` (`status`,`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `dag_node_executions` (
  `id` bigint NOT NULL,
  `dag_execution_id` bigint NOT NULL,
  `node_id` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL,
  `execution_id` bigint DEFAULT NULL,
  `status` tinyint NOT NULL DEFAULT '1' COMMENT '1-等待中 2-运行中 3-成功 4-失败 5-跳过',
  `start_time` datetime(3) DEFAULT NULL,
  `end_time` datetime(3) DEFAULT NULL,
  `input_context` json DEFAULT NULL,
  `output_context` json DEFAULT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_dne_run_node` (`dag_execution_id`,`node_id`),
  KEY `idx_dag_node_executions_dag` (`dag_execution_id`,`status`),
  KEY `idx_dag_node_executions_node` (`node_id`,`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `dag_nodes` (
  `id` bigint NOT NULL,
  `dag_id` bigint NOT NULL,
  `task_id` bigint DEFAULT NULL,
  `node_id` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '节点标识（DAG内唯一）',
  `node_name` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
  `node_type` tinyint NOT NULL DEFAULT '1' COMMENT '1-任务节点 2-条件节点',
  `config` json DEFAULT NULL,
  `sort_order` int NOT NULL DEFAULT '0',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_dag_nodes_dag_node` (`dag_id`,`node_id`),
  KEY `idx_dag_nodes_dag` (`dag_id`,`sort_order`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `dags` (
  `id` bigint NOT NULL,
  `name` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
  `description` varchar(512) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `parallel_limit` int NOT NULL DEFAULT '5',
  `fail_fast` tinyint(1) NOT NULL DEFAULT '1' COMMENT '1-true 0-false',
  `status` tinyint NOT NULL DEFAULT '1' COMMENT '1-启用 2-禁用 3-删除',
  `created_by` bigint NOT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  `deleted_at` datetime(3) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_dags_status_created` (`status`,`created_at`),
  KEY `idx_dags_created_by` (`created_by`,`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `users` (
  `id` bigint NOT NULL,
  `username` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL,
  `email` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `password_hash` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
  `nickname` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `phone` varchar(11) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `role` tinyint NOT NULL DEFAULT '3' COMMENT '1-管理员 2-操作员 3-只读用户',
  `status` tinyint NOT NULL DEFAULT '1' COMMENT '1-正常 2-冻结 3-注销',
  `last_login_at` datetime(3) DEFAULT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  `deleted_at` datetime(3) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_users_username` (`username`),
  UNIQUE KEY `uk_users_email` (`email`),
  KEY `idx_users_status_role` (`status`,`role`,`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

-- 2026-10-14 定时 DAG：调度目标支持任务/DAG（方案：调度直发 dag 运行）
-- ALTER TABLE job_schedule.schedules
--   ADD COLUMN target_type TINYINT NOT NULL DEFAULT 1 COMMENT '触发目标：1-任务 2-DAG' AFTER task_id,
--   ADD COLUMN dag_id BIGINT NULL COMMENT 'DAG ID（target_type=2 时使用）' AFTER target_type;
