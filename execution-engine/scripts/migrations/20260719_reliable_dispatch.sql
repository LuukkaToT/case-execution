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
    `outbox_id`    BIGINT        NOT NULL AUTO_INCREMENT,
    `execution_id` BIGINT        NOT NULL,
    `case_id`      BIGINT        NOT NULL,
    `case_name`    VARCHAR(255)  NOT NULL DEFAULT '',
    `version`      VARCHAR(64)   NOT NULL DEFAULT '',
    `state`        VARCHAR(16)   NOT NULL DEFAULT 'PENDING',
    `attempts`     INT           NOT NULL DEFAULT 0,
    `available_at` DATETIME(3)   NOT NULL,
    `lease_until`  DATETIME(3)       NULL,
    `lease_token`  VARCHAR(64)   NOT NULL DEFAULT '',
    `last_error`   VARCHAR(1024) NOT NULL DEFAULT '',
    `create_at`    DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `update_at`    DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    `published_at` DATETIME(3)       NULL,
    PRIMARY KEY (`outbox_id`),
    UNIQUE KEY `uk_dispatch_outbox_execution` (`execution_id`),
    INDEX `idx_dispatch_outbox_claim` (`state`, `available_at`, `outbox_id`),
    INDEX `idx_dispatch_outbox_reclaim` (`state`, `lease_until`, `outbox_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
