-- =============================================================
--  execution-engine 本地开发库 schema
-- =============================================================

USE case_execution;

CREATE TABLE IF NOT EXISTS `ut_case` (
    `case_id`   BIGINT       NOT NULL AUTO_INCREMENT,
    `case_name` VARCHAR(255) NOT NULL DEFAULT '',
    `version`   VARCHAR(64)  NOT NULL DEFAULT '',
    `channel`   VARCHAR(64)  NOT NULL DEFAULT '',
    PRIMARY KEY (`case_id`),
    -- 单列索引（GORM index tag 对应）
    INDEX `idx_version`  (`version`),
    INDEX `idx_channel`  (`channel`),
    -- 复合索引：让版本扫描与渠道版本扫描都能按 case_id 顺序走范围扫描
    INDEX `idx_version_caseid` (`version`, `case_id`),
    INDEX `idx_channel_version_caseid` (`channel`, `version`, `case_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `execution_record` (
    `execution_id`     BIGINT       NOT NULL AUTO_INCREMENT,
    `request_id`       VARCHAR(64)      NULL,
    `case_id`          BIGINT       NOT NULL DEFAULT 0,
    `version`          VARCHAR(64)  NOT NULL DEFAULT '',
    `create_by`        VARCHAR(64)  NOT NULL DEFAULT '',
    `execution_status` VARCHAR(16)  NOT NULL DEFAULT '',
    `create_at`        DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `execute_at`       DATETIME(3)      NULL,
    `finish_at`        DATETIME(3)      NULL,
    PRIMARY KEY (`execution_id`),
    INDEX `idx_case_id`          (`case_id`),
    INDEX `idx_version`          (`version`),
    INDEX `idx_execution_status` (`execution_status`),
    UNIQUE KEY `uk_execution_request_case` (`request_id`, `case_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `dispatch_outbox` (
    `outbox_id`    BIGINT       NOT NULL AUTO_INCREMENT,
    `execution_id` BIGINT       NOT NULL,
    `case_id`      BIGINT       NOT NULL,
    `case_name`    VARCHAR(255) NOT NULL DEFAULT '',
    `version`      VARCHAR(64)  NOT NULL DEFAULT '',
    `state`        VARCHAR(16)  NOT NULL DEFAULT 'PENDING',
    `attempts`     INT          NOT NULL DEFAULT 0,
    `available_at` DATETIME(3)  NOT NULL,
    `lease_until`  DATETIME(3)      NULL,
    `lease_token`  VARCHAR(64)  NOT NULL DEFAULT '',
    `last_error`   VARCHAR(1024) NOT NULL DEFAULT '',
    `create_at`    DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `update_at`    DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    `published_at` DATETIME(3)      NULL,
    PRIMARY KEY (`outbox_id`),
    UNIQUE KEY `uk_dispatch_outbox_execution` (`execution_id`),
    INDEX `idx_dispatch_outbox_claim` (`state`, `available_at`, `outbox_id`),
    INDEX `idx_dispatch_outbox_reclaim` (`state`, `lease_until`, `outbox_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
