from typing import List, Optional, Dict, Any, Tuple
from sqlalchemy.orm import Session

from ...domain.port import ExecutionRepository
from ...domain.aggregation.vo import Version
from ...domain.aggregation.execution_record import ExecutionRecord
from .dao.table import ExecutionRecordTable
from .dao.execution_dao import ExecutionRecordDao


# 待优化：添加审计日志
class ExecutionRepositoryAdapter(ExecutionRepository):
    def __init__(self):
        self.dao = ExecutionRecordDao()

    def set_session(self, session: Session):
        self.dao.set_session(session)

    def add(self, execution_record: ExecutionRecord):
        db_execution = self.dao.insert(execution_record.to(ExecutionRecordTable))
        if db_execution is None:
            return None
        return db_execution.to(ExecutionRecord)

    def batch_add(self, execution_records: List[ExecutionRecord]):
        db_records = [record.to(ExecutionRecordTable) for record in execution_records]
        db_records = self.dao.batch_insert(db_records)
        if db_records is None:
            return None
        return [db_record.to(ExecutionRecord) for db_record in db_records]

    def page(self, offset: int, limit: int, version: Optional[Version] = None, sort_by: Optional[str] = None,
             order: Optional[str] = "asc") -> List[ExecutionRecord]:
        """查询列表"""
        db_execution_list = self.dao.list(offset, limit, version, sort_by, order)
        execution_list = [db_execution.to(ExecutionRecord) for db_execution in db_execution_list]
        return execution_list

    def find_by_id(self, execution_id: int) -> ExecutionRecord | None:
        """根据ID查询执行记录"""
        db_execution = self.dao.select_by_id(execution_id)
        if db_execution is None:
            return None
        return db_execution.to(ExecutionRecord)

    def save(self, execution_record: ExecutionRecord):
        """保存更新后的信息"""
        db_execution = self.dao.update(execution_record.to(ExecutionRecordTable))
        if db_execution is None:
            return None
        return db_execution.to(ExecutionRecord)

    def query_case_execution_info_direct(self, offset: int, limit: int, version: Optional[Version] = None,
                                         sort_by: Optional[str] = None, ):
        """直接查询包含用例名以及用例信道的用例执行结果"""
        row_list = self.dao.list_by_case_name_channel(offset, limit, version, sort_by)
        return row_list

    def query_executions(self, offset: int, limit: int, version: Optional[Version] = None,
                         sort_by: Optional[str] = None, order: Optional[str] = "asc",
                         execution_status: Optional[List[str]] = None,
                         fields: Optional[List[str]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """
                统一查询接口
                返回: 字典列表，key为字段名
                """
        rows, total, field_names = self.dao.query_dynamic(offset, limit, version, sort_by, order, execution_status,
                                                          fields)

        # 将 Row 对象转为 Dict 列表
        result_list = []
        for row in rows:
            # row 是 SQLAlchemy 的 Row 对象，可以直接转 dict (依赖 sqlalchemy 版本)
            item = dict(zip(field_names, row))
            result_list.append(item)

        return result_list, total

    def batch_update_status(self, records: List[ExecutionRecord]):
        db_record = [record.to(ExecutionRecordTable) for record in records]
        self.dao.batch_update_status(db_record)  这是我的执行用例repository仓储
