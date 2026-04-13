from dataclasses import dataclass
from typing import List, Tuple, Dict

from . import CaseNotExistError
from ..aggregation import UtCase
from ..aggregation.vo import Version, BatchResult
from ..aggregation.vo.execution_task import ExecutionTask
from ..port import ExecutionRepository, UtCaseRepository, UtCaseExecutorClient
from ..aggregation import ExecutionRecord


@dataclass
class ExecutionService:
    case_repo: UtCaseRepository
    execution_repo: ExecutionRepository
    executor_client: UtCaseExecutorClient

    def create_execution_record(self, case_id: int, version: Version, user: str):
        """
        创建用例执行记录
        """
        return ExecutionRecord.create(case_id=case_id, version=version, user=user)

    def execute(self, execution_record: ExecutionRecord, case_name: str):
        success = self.executor_client.execute(execution_record, case_name)
        return success

    def create_batch_records(self, ut_cases: List[UtCase], version: Version, user: str) -> Tuple[
        List[ExecutionRecord], Dict[int, str]]:
        """
        第一步：创建执行记录，以及case_id和名字的映射
        """
        execution_records = []
        name_map = {}

        for case in ut_cases:
            record = ExecutionRecord.create(
                case_id=case.case_id,
                version=version,
                user=user
            )

            execution_records.append(record)
            name_map[case.case_id] = case.case_name  # 建立 ID -> Name 的映射

        return execution_records, name_map

    def dispatch_batch(self, records: List[ExecutionRecord], tasks: List[ExecutionTask]) -> None:
        """
        第二步：批量下发并处理结果
        """
        if not records:
            return

        # 这里可能抛出网络异常，由 App 层捕获
        result: List[BatchResult] = self.executor_client.batch_run(tasks)

        failed_id_set = set(result.failed_ids)
        success_id_set = set(result.success_ids)

        for record in records:
            if record.execution_id in failed_id_set:
                record.mark_as_failed()  # 或者 mark_as_submit_fail
                # 还可以记录 result.error_message
            elif record.execution_id in success_id_set:
                record.mark_as_wait()
            else:
                pass  这是我的执行阶段领域服务