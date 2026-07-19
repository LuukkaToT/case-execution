-- 适用于已经存在旧版 execution_record、ut_case 表的环境，只执行一次。
-- 全新环境直接使用 scripts/schema.sql，不再执行本迁移。

USE case_execution;

ALTER TABLE `execution_record`
    ADD COLUMN `request_id` VARCHAR(64) NULL AFTER `execution_id`,
    ADD UNIQUE INDEX `uk_execution_request_case` (`request_id`, `case_id`);

ALTER TABLE `ut_case`
    ADD INDEX `idx_version_caseid` (`version`, `case_id`),
    ADD INDEX `idx_channel_version_caseid` (`channel`, `version`, `case_id`);

CREATE TABLE `dispatch_outbox` (
    `outbox_id`    BIGINT        NOT NULL AUTO_INCREMENT COMMENT 'Outbox 自增主键',
    `execution_id` BIGINT        NOT NULL COMMENT '执行记录编号与消息幂等键',
    `case_id`      BIGINT        NOT NULL COMMENT '用例编号快照',
    `case_name`    VARCHAR(255)  NOT NULL DEFAULT '' COMMENT '用例名称快照',
    `version`      VARCHAR(64)   NOT NULL DEFAULT '' COMMENT '版本快照与 RabbitMQ 路由键',
    `state`        VARCHAR(16)   NOT NULL DEFAULT 'PENDING' COMMENT '投递状态 PENDING PROCESSING PUBLISHED DEAD',
    `attempts`     INT           NOT NULL DEFAULT 0 COMMENT '成功领取租约的累计次数',
    `available_at` DATETIME(3)   NOT NULL COMMENT '下一次允许领取时间',
    `lease_until`  DATETIME(3)       NULL COMMENT '当前处理租约到期时间',
    `lease_token`  VARCHAR(64)   NOT NULL DEFAULT '' COMMENT '本次领取令牌 防止旧实例迟到写回',
    `last_error`   VARCHAR(1024) NOT NULL DEFAULT '' COMMENT '最近一次发布失败摘要',
    `create_at`    DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '消息创建时间',
    `update_at`    DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '消息最后更新时间',
    `published_at` DATETIME(3)       NULL COMMENT '发布确认写回时间',
    PRIMARY KEY (`outbox_id`),
    UNIQUE KEY `uk_dispatch_outbox_execution` (`execution_id`),
    INDEX `idx_dispatch_outbox_claim` (`state`, `available_at`, `outbox_id`),
    INDEX `idx_dispatch_outbox_reclaim` (`state`, `lease_until`, `outbox_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='用例下发可靠投递 Outbox';
