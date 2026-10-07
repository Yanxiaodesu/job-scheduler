-- ============================================================
--  分布式定时任务调度平台 —— 表结构
--
--  只建 4 张表。xxl-job 有十几张，我们砍到只剩调度必需的部分。
--
--  ## 为什么没有「调度锁」表 / 不用 Redis 分布式锁
--
--  这是这个项目最重要的一个设计决定：**恰好执行一次**不靠引入锁组件，
--  靠数据库自己的两个特性就够了 ——
--
--    第一层：UPDATE job_info SET next_trigger_time = 下次
--            WHERE id = ? AND next_trigger_time = 旧值
--            → 乐观锁。多个 Admin 同时扫到同一个任务时，
--              只有一个人的 UPDATE 能影响到 1 行，其他人 rowcount = 0 直接跳过。
--
--    第二层：job_instance 上的 UNIQUE KEY (instance_key)
--            instance_key = "jobId:触发时刻"
--            → 就算第一层因为某种原因没拦住，同一任务同一时刻的实例也只能落一条。
--
--  这样做的收益：少一个组件、少一个故障点，本地就能跑起来。
--  代价：并发很高时会有较多「乐观锁失败」的重试；强依赖数据库可用性。
--  生产环境该用 Redis 还是数据库锁，取决于并发量 —— 这个取舍要能在面试里讲清楚。
--
--  和 xxl-job 的其他差异（都是刻意的）：
--    · 不做 GLUE 在线脚本，只支持「HTTP 回调」和「内置 handler」
--    · 不做分片广播，只做单机路由（轮询 / 随机）
--    · 不做告警，失败只留痕
-- ============================================================

CREATE DATABASE IF NOT EXISTS job_scheduler
  CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
USE job_scheduler;

-- ------------------------------------------------------------
-- 1. 任务定义
-- ------------------------------------------------------------
DROP TABLE IF EXISTS job_info;
CREATE TABLE job_info (
  id                BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  job_name          VARCHAR(128) NOT NULL COMMENT '任务名，唯一',
  cron_expr         VARCHAR(64)  NOT NULL COMMENT 'cron 表达式（Spring 6 段式）',
  handler_type      TINYINT      NOT NULL DEFAULT 1 COMMENT '1内置handler / 2 HTTP回调',
  handler_value     VARCHAR(512) NOT NULL COMMENT '内置 handler 名 或 回调 URL',
  param             VARCHAR(512) DEFAULT NULL COMMENT '传给 handler 的参数，如 sleep 的秒数',
  timeout_sec       INT          NOT NULL DEFAULT 30 COMMENT '单次执行超时',
  max_retry         INT          NOT NULL DEFAULT 0 COMMENT '失败最多重试几次',
  misfire_strategy  TINYINT      NOT NULL DEFAULT 2 COMMENT '1补跑一次 / 2直接跳过',
  status            TINYINT      NOT NULL DEFAULT 1 COMMENT '0停用 / 1启用',
  -- ★ 调度循环就靠这一列扫描：WHERE status=1 AND next_trigger_time <= NOW()
  --   建索引是必须的，否则任务一多就全表扫
  -- ★ 时间列统一用 DATETIME(3)（毫秒精度），不是 DATETIME。
  --   原因：这个项目的核心指标之一是「调度精度」（实际触发 - 计划触发）。
  --   如果只存到秒，两个整秒相减永远是 0，根本测不出精度。
  --   调度系统记录时间戳本来就该到毫秒。
  next_trigger_time DATETIME(3)  DEFAULT NULL,
  create_time       DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  update_time       DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_job_name (job_name),
  KEY idx_due (status, next_trigger_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='任务定义';

-- ------------------------------------------------------------
-- 2. 执行器注册表
-- ------------------------------------------------------------
DROP TABLE IF EXISTS executor_registry;
CREATE TABLE executor_registry (
  id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  app_name       VARCHAR(64)  NOT NULL COMMENT '执行器分组名',
  address        VARCHAR(128) NOT NULL COMMENT '形如 http://127.0.0.1:9001',
  last_heartbeat DATETIME     NOT NULL COMMENT '最近一次心跳',
  status         TINYINT      NOT NULL DEFAULT 1 COMMENT '1在线 / 0离线',
  create_time    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uk_addr (app_name, address),
  KEY idx_heartbeat (status, last_heartbeat)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='执行器注册表';

-- ------------------------------------------------------------
-- 3. 任务实例：每次「触发」产生一行
-- ------------------------------------------------------------
DROP TABLE IF EXISTS job_instance;
CREATE TABLE job_instance (
  id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  job_id        BIGINT UNSIGNED NOT NULL,
  executor_id   BIGINT UNSIGNED DEFAULT NULL COMMENT '被派发到的执行器',
  trigger_time  DATETIME(3)  NOT NULL COMMENT '计划触发时刻（同一 job 同一时刻唯一）',
  instance_key  VARCHAR(128) NOT NULL COMMENT 'jobId:triggerTime，用于幂等',
  start_time    DATETIME(3)  DEFAULT NULL,
  end_time      DATETIME(3)  DEFAULT NULL,
  status        TINYINT      NOT NULL DEFAULT 0 COMMENT '0待执行 1执行中 2成功 3失败 4超时 5已跳过',
  retry_count   INT          NOT NULL DEFAULT 0,
  result_msg    VARCHAR(1000) DEFAULT NULL,
  create_time   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  -- ★ 防止同一任务同一触发时刻产生两条实例（配合 Redis/DB 锁做双保险）
  UNIQUE KEY uk_instance (instance_key),
  KEY idx_status (status),
  KEY idx_job (job_id, trigger_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='任务实例';

-- ------------------------------------------------------------
-- 4. 执行日志
-- ------------------------------------------------------------
DROP TABLE IF EXISTS job_log;
CREATE TABLE job_log (
  id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  instance_id BIGINT UNSIGNED NOT NULL,
  log_time    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  level       VARCHAR(8) NOT NULL DEFAULT 'INFO',
  content     VARCHAR(1000) NOT NULL,
  PRIMARY KEY (id),
  KEY idx_instance (instance_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='执行日志';
