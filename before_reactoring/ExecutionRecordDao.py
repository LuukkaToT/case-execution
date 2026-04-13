from typing import Optional, List, Tuple
from sqlalchemy import desc, select, asc, func
from sqlalchemy.orm import Session
from sqlalchemy.engine import Row
from .mapping import FIELD_MAP

from app.core.multicore_concurrent.domain.aggregation import ExecutionRecord
from app.core.multicore_concurrent.south.repository.dao.mapping import DEFAULT_EXECUTION_FIELDS
from app.core.multicore_concurrent.south.repository.dao.table import ExecutionRecordTable, UtCaseTable


class ExecutionRecordDao:
    def __init__(self):
        self._session: Optional[Session] = None

    def set_session(self, session: Session):
        self._session = session

    def insert(self, record: ExecutionRecordTable):
        self._session.add(record)
        self._session.flush()
        return record

    def batch_insert(self, records: List[ExecutionRecordTable]):
        self._session.add_all(records)
        self._session.flush()
        return records

    def select_by_id(self, execution_id: int) -> ExecutionRecordTable:
        db_record = self._session.get(ExecutionRecordTable, execution_id)
        if db_record is None:
            raise Exception("Record not found in database")
        return db_record

    def select_by_case_id(self, case_id: int) -> List[ExecutionRecord]:
        stmt = select(ExecutionRecordTable).where(ExecutionRecordTable.case_id == case_id)
        case_list = self._session.scalars(stmt).all()
        return case_list

    def update(self, record: ExecutionRecordTable):
        db_record = self._session.get(ExecutionRecordTable, record.execution_id)
        if db_record is None:
            raise Exception("Record not found in database!")
        db_record.execution_status = record.execution_status
        db_record.execute_at = record.execute_at
        db_record.finish_at = record.finish_at
        self._session.flush()
        return record

    def list(self, offset: int, limit: int, version: Optional[str] = None, sort_by: Optional[str] = None,
             order: Optional[str] = "asc", ) -> List[ExecutionRecordTable]:
        """返回用例列表，使用用例id"""
        # 筛选，根据是否传入版本进行筛选，不传入版本默认显示全部
        if version is not None:
            stmt = select(ExecutionRecordTable).where(ExecutionRecordTable.version == version)
        else:
            stmt = select(ExecutionRecordTable)

        # 排序
        if sort_by and hasattr(ExecutionRecordTable, sort_by):
            column = getattr(ExecutionRecordTable, sort_by)
            stmt = stmt.order_by(asc(column) if order == "asc" else desc(column))

        # 分页
        stmt = stmt.offset(offset).limit(limit)

        case_list = list(self._session.scalars(stmt).all())
        return case_list

    def list_by_case_name_channel(self, offset: int, limit: int, version: Optional[str] = None,
                                  sort_by: Optional[str] = None, order: Optional[str] = "asc") -> List[Row]:
        """返回一个列表，因为使用了join,此处返回的类型会附加一列新的case_name"""
        stmt = select(ExecutionRecordTable, UtCaseTable.case_name.label('case_name'),
                      UtCaseTable.channel.label('channel')) \
            .join(UtCaseTable, ExecutionRecordTable.case_id == UtCaseTable.case_id)

        # 筛选版本
        if version is not None:
            stmt = stmt.where(ExecutionRecordTable.version == version)

        if sort_by and hasattr(ExecutionRecordTable, sort_by):
            column = getattr(ExecutionRecordTable, sort_by)
            stmt = stmt.order_by(asc(column) if order == "asc" else desc(column))

        stmt = stmt.offset(offset).limit(limit)

        case_list = list(self._session.execute(stmt).all())
        return case_list

    def query_dynamic(self, offset: int, limit: int, version: Optional[str] = None,
                      sort_by: Optional[str] = None, order: Optional[str] = "asc",
                      execution_status: Optional[List[str]] = None,
                      fields: Optional[List[str]] = None) -> Tuple[List[Row], int, List[str]]:
        """动态查询方法"""
        target_fields = fields if fields else DEFAULT_EXECUTION_FIELDS

        selected_columns = []
        final_field_names = []
        need_join = False

        for field in target_fields:
            if field in FIELD_MAP:
                col, is_join_field = FIELD_MAP[field]
                selected_columns.append(col.label(field))
                final_field_names.append(field)
                if is_join_field:
                    need_join = True

        if not selected_columns:
            selected_columns.append(ExecutionRecordTable.execution_id.label("execution_id"))
            final_field_names.append("execution_id")

        # --- 1. 构建查询主体 ---
        stmt = select(*selected_columns)

        # Count 语句---
        count_stmt = select(func.count()).select_from(ExecutionRecordTable)

        # --- 2. 处理 Join ---
        if need_join:
            stmt = stmt.join(UtCaseTable, ExecutionRecordTable.case_id == UtCaseTable.case_id)
            # 注意：如果过滤条件不涉及 UtCaseTable，count_stmt 其实可以不加 join，性能更好。
            # 但为了逻辑严谨（防止未来加了跨表过滤），这里保持一致也行。
            count_stmt = count_stmt.join(UtCaseTable, ExecutionRecordTable.case_id == UtCaseTable.case_id)

        # --- 3. 处理筛选 ---
        if version:
            stmt = stmt.where(ExecutionRecordTable.version == version)
            count_stmt = count_stmt.where(ExecutionRecordTable.version == version)

        if execution_status:
            stmt = stmt.where(ExecutionRecordTable.execution_status.in_(execution_status))
            count_stmt = count_stmt.where(ExecutionRecordTable.execution_status.in_(execution_status))

        # 执行 Count
        total = self._session.scalar(count_stmt) or 0

        # --- 4. 排序 ---
        if sort_by:
            sort_col_info = FIELD_MAP.get(sort_by)
            if sort_col_info:
                sort_col = sort_col_info[0]
                stmt = stmt.order_by(asc(sort_col) if order == "asc" else desc(sort_col))

        # --- 5. 分页 ---
        stmt = stmt.offset(offset).limit(limit)

        # --- 6. 执行查询 ---
        rows = self._session.execute(stmt).all()

        return rows, total, final_field_names

    def batch_update_status(self, records: List[ExecutionRecord]):
        """批量更新状态优化"""
        mappings = [
            {"execution_id": r.execution_id, "execution_status": r.execution_status}
            for r in records
        ]
        self._session.bulk_update_mappings(ExecutionRecordTable, mappings)这是仓储调用的dao