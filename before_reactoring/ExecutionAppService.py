from traceback import format_exc
from typing import List, Optional

from app.common.utils.db import session_scope
from app.common.utils import file_logger as logger
from app.common.utils.iterators import chunk_list
from app.core.multicore_concurrent.south.repository.execution_repository_adapter import ExecutionRepositoryAdapter
from ...domain.aggregation.vo import Version, ExecutionStatus, Channel
from ...domain.aggregation.vo.execution_task import ExecutionTask
from ...domain.service import ExecutionNotFoundError
from ...domain.aggregation import UtCase
from ...domain.service.execution_service import ExecutionService
from ...south.repository.ut_case_repository_adapter import UtCaseRepositoryAdapter

from .exceptions import *


class ExecutionAppService:
    def __init__(self, executor_client) -> None:
        self.execution_repo = ExecutionRepositoryAdapter()
        self.case_repo = UtCaseRepositoryAdapter()
        self.executor_client = executor_client
        self.execution_service = ExecutionService(self.case_repo, self.execution_repo, self.executor_client)

    def set_session(self, session):
        self.execution_repo.set_session(session)
        self.case_repo.set_session(session)

    def execute_case(self, case_id: int, version: Version, user: str):
        """执行用例"""
        execution_id = None
        case_name = None

        try:
            with session_scope() as session:
                self.set_session(session)

                # 业务校验，校验用例id是否存在
                ut_case = self.case_repo.find_by_id(case_id)
                if ut_case is None:
                    raise CaseNotExistError(f"当前用例:{case_id}不存在!")

                # 创建执行记录
                execution_record = self.execution_service.create_execution_record(case_id, version, user)
                # 执行记录保存到数据库
                execution_record = self.execution_repo.add(execution_record)

                # 保存后续事务所需要的必要信息
                execution_id = execution_record.execution_id
                case_name = ut_case.case_name
        except Exception as e:
            logger.error(f"创建用例执行记录失败,执行记录id:{execution_id},错误:{format_exc()}")
            raise e

        # 消息下发到rabbitMQ
        ret = False
        try:
            ret = self.execution_service.execute(execution_record, case_name)
        except Exception as e:
            ret = False
            logger.error(f"执行客户端调用异常: {e}")

        try:
            with session_scope() as session:
                self.set_session(session)
                curr_record = self.execution_repo.find_by_id(execution_id)
                if curr_record is None:
                    raise ExecutionNotFoundError(f"执行记录未找到，execution_id:{execution_id}")
                if ret:
                    curr_record.mark_as_wait()
                else:
                    curr_record.mark_as_failed()
                self.execution_repo.save(curr_record)
        except Exception as e:
            logger.error(f"更新用例执行状态异常:{curr_record.execution_id},错误:{format_exc()}")
            raise e

    def execute_all_cases(self, version: Version, user: str):
        """场景1：全量执行"""
        with session_scope() as session:
            self.set_session(session)
            # 获取全部用例
            all_cases = self.case_repo.find_by_version(version)

        # 调用批量执行逻辑
        return self._execute_batch_logic(all_cases, version, user)

    def execute_channel_cases(self, channel: Channel, version: Version, user: str):
        """场景2：按信道执行"""
        with session_scope() as session:
            self.set_session(session)
            # 1. 获取数据
            channel_cases = self.case_repo.find_by_channel_version(channel=channel, version=version)

        # 2. 调用通用引擎
        return self._execute_batch_logic(cases=channel_cases, version=version, user=user)

    def _execute_batch_logic(self, cases: List[UtCase], version: Version, user: str):
        """批量执行用例"""
        BATCH_SIZE = 1000

        # 使用 chunk_list 进行分批循环
        for batch_cases in chunk_list(cases, BATCH_SIZE):
            # 开启一个事务
            with session_scope() as session:
                self.set_session(session)
                session.expire_on_commit = False

                # 分批创建用例执行记录
                records, name_map = self.execution_service.create_batch_records(batch_cases, version, user)
                # 执行记录批量写入数据库，并回填execution_id
                records = self.execution_repo.batch_add(records)

                tasks = ExecutionTask.batch_assemble(records, name_map)

            try:
                self.execution_service.dispatch_batch(records, tasks)
            except Exception as e:
                # 记录失败的部分用例即可，并且将用例执行状态标记为fail
                logger.error(f"批量发送 MQ 失败: {e}")
                continue

            with session_scope() as session:
                self.set_session(session)
                self.execution_repo.batch_update_status(records)

            logger.info(f"成功下发分片，数量: {len(batch_cases)}")

    def update_execution_status(self, execution_id: int, execution_status: ExecutionStatus):
        """更新用例的执行状态,非终态(RUNNING)"""
        try:
            with session_scope() as session:
                self.set_session(session)

                # 查找用例执行记录
                execution_record = self.execution_repo.find_by_id(execution_id)
                if execution_record is None:
                    raise ExecutionNotFoundError(f"当前执行记录:{execution_id}不存在")

                # 调用实体方法进行状态转换
                if execution_status == ExecutionStatus.RUNNING:
                    execution_record.mark_as_running()
                else:
                    execution_record.modify_execution_status(execution_status)

                # 存入数据库
                self.execution_repo.save(execution_record)
        except Exception as e:
            logger.error(f"更新用例执行状态失败,执行记录id{execution_record.execution_id},错误:{format_exc()}")
            raise e

    def search_executions(self, offset: int, limit: int, version: Optional[Version] = None,
                          sort_by: Optional[str] = None, execution_status: Optional[List[str]] = None,
                          fields: Optional[List[str]] = None):
        """分页查询执行记录，显示用例名"""
        try:
            with session_scope() as session:
                self.set_session(session)

                # 调用 Repo
                data_list, total = self.execution_repo.query_executions(
                    offset=offset,
                    limit=limit,
                    version=version,
                    sort_by=sort_by,
                    execution_status=execution_status,
                    fields=fields
                )

                return data_list, total
        except Exception as e:
            logger.error(f"查询用例失败,错误:{format_exc()}")
            raise e    这是我的app服务。