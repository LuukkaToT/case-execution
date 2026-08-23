-- 执行记录不再使用 INIT 占位。投递进度由 dispatch_outbox 维护，
-- 新建执行记录直接落 WAIT；历史 INIT 行视为尚未被执行机接手。
UPDATE `execution_record`
SET `execution_status` = 'WAIT'
WHERE `execution_status` = 'INIT';
