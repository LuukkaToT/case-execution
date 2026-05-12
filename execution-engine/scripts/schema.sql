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
    -- 复合索引：让 WHERE version=? ORDER BY case_id 走 range scan
    INDEX `idx_version_caseid` (`version`, `case_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `execution_record` (
    `execution_id`     BIGINT       NOT NULL AUTO_INCREMENT,
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
    INDEX `idx_execution_status` (`execution_status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
