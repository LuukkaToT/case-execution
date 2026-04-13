import pika
import json
import threading
from typing import List

from app.config import settings
from app.common.utils import file_logger as logger
from ...domain.port import UtCaseExecutorClient
from ...domain.aggregation import ExecutionRecord
from ...domain.aggregation.vo.execution_task import ExecutionTask
from ...domain.aggregation.vo import BatchResult


class UtCaseExecutorClientAdapter(UtCaseExecutorClient):
    """
    基础设施层适配器：
    基于 Pika 的 RabbitMQ 客户端。
    """

    def __init__(self):
        # 1. 加载配置
        self.host = settings.RABBITMQ_HOST
        self.port = settings.RABBITMQ_PORT
        self.username = settings.RABBITMQ_DEFAULT_USER
        self.password = settings.RABBITMQ_DEFAULT_PASS
        self.exchange_name = settings.RABBITMQ_EXCHANGE

        # 初始化线程锁
        # pika.BlockingConnection 不是线程安全的，必须加锁互斥访问
        self._lock = threading.Lock()

        # 连接对象
        self._connection = None
        self._channel = None

        # 尝试建立初始连接
        # 注意：这里不需要加锁，因为 __init__ 只会被主线程调用一次（单例模式下）
        self._ensure_connection()

    def _ensure_connection(self):
        """
        [内部方法] 确保连接可用。
        注意：此方法应在已获取锁的上下文中被调用。
        """
        if self._connection and not self._connection.is_closed and self._channel and not self._channel.is_closed:
            return

        try:
            logger.info(f"[MQ] 正在建立连接 {self.host}:{self.port} ...")
            credentials = pika.PlainCredentials(self.username, self.password)
            parameters = pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                credentials=credentials,
                socket_timeout=5,  # Socket 超时
                heartbeat=60,  # 心跳保活
                blocked_connection_timeout=300  # 阻塞超时
            )
            self._connection = pika.BlockingConnection(parameters)
            self._channel = self._connection.channel()

            # 声明 Exchange
            self._channel.exchange_declare(
                exchange=self.exchange_name,
                exchange_type='direct',
                durable=True
            )

            # 开启 Publisher Confirms (确保数据不丢失)
            self._channel.confirm_delivery()
            logger.info("[MQ] 连接建立成功")

        except Exception as e:
            logger.error(f"[MQ] 连接建立失败: {repr(e)}")
            self._connection = None
            self._channel = None

    def _publish_internal(self, payload: dict, routing_key: str):
        """
        [内部方法] 执行实际的 Basic Publish。
        注意：此方法应在已获取锁的上下文中被调用。
        """
        body = json.dumps(payload, ensure_ascii=False)

        self._channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=routing_key,
            body=body.encode('utf-8'),
            properties=pika.BasicProperties(
                delivery_mode=2,  # 持久化
                content_type='application/json'
            )
        )

    def batch_run(self, tasks: List[ExecutionTask]) -> BatchResult:
        """
        [批量接口] 线程安全的批量发送
        """
        success_ids = []
        failed_ids = []

        # 在这整个代码块执行期间，其他线程无法使用 MQ 连接
        with self._lock:
            # 1. 检查连接（如果断开则重连）
            self._ensure_connection()

            if not self._connection or not self._channel:
                return BatchResult(
                    success_ids=[],
                    failed_ids=[t.execution_id for t in tasks],
                    error_message="MQ Connection failed"
                )

            # 2. 循环发送
            for task in tasks:
                try:
                    payload = {
                        "execution_id": task.execution_id,
                        "case_name": task.case_name,
                        "version": task.version
                    }
                    routing_key = task.version

                    self._publish_internal(payload, routing_key)
                    success_ids.append(task.execution_id)

                except Exception as e:
                    logger.error(f"[MQ] 任务 {task.execution_id} 发送异常: {e}")
                    failed_ids.append(task.execution_id)

        return BatchResult(success_ids=success_ids, failed_ids=failed_ids)

    def execute(self, execution_record: ExecutionRecord, case_name: str) -> bool:
        """
        [单条接口] 线程安全的单条发送
        """
        with self._lock:
            self._ensure_connection()

            if not self._connection or not self._channel:
                return False

            try:
                payload = {
                    "execution_id": execution_record.execution_id,
                    "case_name": case_name,
                    "version": execution_record.version
                }
                routing_key = execution_record.version

                self._publish_internal(payload, routing_key)
                logger.info(f"[MQ] 单条发送成功 ID={execution_record.execution_id},case_name={case_name},version={execution_record.version},create_by={execution_record.create_by}")
                return True

            except Exception as e:
                logger.error(f"[MQ] 单条发送失败: {repr(e)}")
                # 遇到错误尝试重置连接，以便下次重试
                try:
                    if self._connection and not self._connection.is_closed:
                        self._connection.close()
                except:
                    pass
                self._connection = None
                return False

    def close(self):
        """
        [生命周期管理] 显式关闭连接
        """
        with self._lock:
            try:
                if self._connection and not self._connection.is_closed:
                    logger.info("[MQ] 正在断开连接...")
                    self._connection.close()
                    logger.info("[MQ] 连接已断开")
            except Exception as e:
                logger.warning(f"[MQ] 关闭连接时发生错误: {e}")
            finally:
                self._connection = None
                self._channel = None      这是下发用例过程中的消息队列发送逻辑。