-- =============================================================
--  生成测试数据：version='26B' 共 52265 条，分 5 个 channel
--  用笛卡尔积一次性生成序列，比存储过程逐行插入快很多
-- =============================================================

USE case_execution;

-- 清空旧数据（bench 多次跑时保证幂等）
SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE execution_record;
TRUNCATE TABLE ut_case;
SET FOREIGN_KEY_CHECKS = 1;

-- 用 cross join 生成 0..99999 序列，再过滤到 < 52265
INSERT INTO ut_case (case_name, version, channel)
SELECT
    CONCAT('case_', LPAD(n + 1, 6, '0')) AS case_name,
    '26B'                                  AS version,
    CONCAT('ch', (n MOD 5) + 1)           AS channel
FROM (
    SELECT
        a.n + b.n * 10 + c.n * 100 + d.n * 1000 + e.n * 10000 AS n
    FROM
        (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
              UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
        (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
              UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
        (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
              UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) c,
        (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
              UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) d,
        (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
              UNION SELECT 5 UNION SELECT 6) e
    WHERE a.n + b.n * 10 + c.n * 100 + d.n * 1000 + e.n * 10000 < 52265
) seq;

SELECT COUNT(*) AS total_cases, version FROM ut_case GROUP BY version;
