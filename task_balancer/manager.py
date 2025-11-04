from typing import Any, List, Callable, Dict, Optional
import asyncio
import time
from dataclasses import dataclass
from enum import Enum
import concurrent.futures
from task_balancer.utils.log_helper import logger


class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    WAITING_FOR_RESOURCE = "waiting_for_resource"


@dataclass
class TaskInfo:
    id: str
    kwargs: Dict[str, Any]
    status: TaskStatus
    result: Any = None
    error: Optional[Exception] = None
    server_id: Optional[str] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    retry_count: int = 0
    last_failed_server: Optional[str] = None


class AsyncTaskQueueManager:
    def __init__(
        self,
        task_function: Callable,
        server_param_name: str,
        available_server_ids: List[Any],
        max_parallel_tasks: int = 20,
        max_retries: int = 3,
    ):
        self.task_function = task_function
        self.server_param_name = server_param_name
        self.available_server_ids = available_server_ids
        self.max_parallel_tasks = max_parallel_tasks
        self.max_retries = max_retries

        # 任务管理
        self.tasks: Dict[str, TaskInfo] = {}
        self._task_id_counter = 0

        # 重试管理
        self._retry_queue: asyncio.Queue = asyncio.Queue()
        self._retry_event = asyncio.Event()  # 用于通知有任务完成

        # 服务器状态
        self.server_stats = {server_id: 0 for server_id in available_server_ids}
        self.server_active_tasks = {server_id: 0 for server_id in available_server_ids}
        self.server_error_count = {server_id: 0 for server_id in available_server_ids}

        # 执行器和控制
        self._semaphore = asyncio.Semaphore(max_parallel_tasks)
        self._thread_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_parallel_tasks
        )
        self._retry_monitor_task: Optional[asyncio.Task] = None
        self._is_running = False

    async def start(self):
        """启动重试监控器"""
        if self._is_running:
            return

        self._is_running = True
        self._retry_monitor_task = asyncio.create_task(self._retry_monitor_loop())
        logger.info("🔧 任务管理器已启动")

    async def stop(self):
        """停止管理器"""
        if not self._is_running:
            return

        self._is_running = False
        if self._retry_monitor_task:
            self._retry_monitor_task.cancel()
            try:
                await self._retry_monitor_task
            except asyncio.CancelledError:
                pass
        self._thread_pool.shutdown(wait=True)
        logger.info("🔧 任务管理器已停止")

    async def submit_single_task(
        self, task_kwargs: Dict[str, Any], task_id: str = None
    ) -> Any:
        """提交单个任务并直接返回结果"""
        if not self._is_running:
            raise RuntimeError("任务管理器未启动，请先调用 start() 方法")

        if task_id is None:
            self._task_id_counter += 1
            task_id = f"task_{self._task_id_counter}"

        if task_id in self.tasks:
            raise ValueError(f"任务ID {task_id} 已存在")

        task_info = TaskInfo(id=task_id, kwargs=task_kwargs, status=TaskStatus.PENDING)
        self.tasks[task_id] = task_info

        logger.info("📤 提交任务 %s", task_id)
        await self._print_status()

        try:
            # 获取信号量，但不要持有它等待重试
            await self._semaphore.acquire()
            try:
                result = await self._execute_task_with_smart_retry(task_info)
                return result
            finally:
                # 确保信号量被释放
                self._semaphore.release()
        except Exception as e:
            task_info.status = TaskStatus.FAILED
            task_info.error = e
            task_info.end_time = time.time()
            # 确保信号量被释放
            if self._semaphore.locked():
                self._semaphore.release()
            raise e

    async def _execute_task_with_smart_retry(self, task_info: TaskInfo) -> Any:
        """执行任务，使用智能重试策略"""
        original_kwargs = task_info.kwargs.copy()

        while task_info.retry_count <= self.max_retries:
            try:
                # 选择最优服务器（避开最近失败的服务器）
                server_id = self._get_optimal_server(task_info.last_failed_server)

                # 执行任务
                return await self._execute_single_attempt(
                    task_info, server_id, original_kwargs
                )

            except Exception as e:
                task_info.retry_count += 1
                task_info.last_failed_server = task_info.server_id

                if task_info.retry_count > self.max_retries:
                    # 最终失败
                    task_info.status = TaskStatus.FAILED
                    task_info.error = e
                    task_info.end_time = time.time()
                    self.server_error_count[task_info.server_id] += 1
                    logger.info("💥 任务 %s 最终失败", task_info.id)
                    await self._print_status()
                    raise e

                # 将任务加入重试队列，等待有任务完成
                task_info.status = TaskStatus.WAITING_FOR_RESOURCE
                await self._retry_queue.put(task_info)
                logger.info(
                    "🔄 任务 %s 加入重试队列 (重试 %d/%d)",
                    task_info.id,
                    task_info.retry_count,
                    self.max_retries,
                )
                await self._print_status()

                # 释放信号量，等待有任务完成（资源释放）
                self._semaphore.release()
                try:
                    await self._wait_for_task_completion()
                finally:
                    # 重新获取信号量继续执行
                    await self._semaphore.acquire()

    async def _execute_single_attempt(
        self, task_info: TaskInfo, server_id: str, original_kwargs: Dict
    ) -> Any:
        """执行单次任务尝试"""
        # 更新任务状态
        task_info.status = TaskStatus.RUNNING
        task_info.server_id = server_id
        task_info.start_time = time.time()
        self.server_active_tasks[server_id] += 1

        logger.info(
            "🔄 任务 %s 在服务器 %s 上执行 (尝试 %d/%d)",
            task_info.id,
            server_id,
            task_info.retry_count + 1,
            self.max_retries + 1,
        )
        await self._print_status()

        try:
            # 执行任务
            task_kwargs = original_kwargs.copy()
            task_kwargs[self.server_param_name] = server_id

            if asyncio.iscoroutinefunction(self.task_function):
                result = await self.task_function(**task_kwargs)
            else:
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(
                    self._thread_pool, lambda: self.task_function(**task_kwargs)
                )

            # 任务成功完成
            task_info.status = TaskStatus.COMPLETED
            task_info.result = result
            task_info.end_time = time.time()
            self.server_stats[server_id] += 1

            duration = task_info.end_time - task_info.start_time
            logger.info("✅ 任务 %s 完成 (耗时: %.2fs)", task_info.id, duration)

            # 通知重试监控器有任务完成
            self._signal_task_completion()
            await self._print_status()

            return result

        except Exception as e:
            self.server_error_count[server_id] += 1
            logger.info("❌ 任务 %s 在服务器 %s 上失败: %s", task_info.id, server_id, e)
            raise e

        finally:
            # 清理活跃任务计数
            if server_id in self.server_active_tasks:
                self.server_active_tasks[server_id] = max(
                    0, self.server_active_tasks[server_id] - 1
                )

    def _signal_task_completion(self):
        """通知有任务完成（资源释放）"""
        self._retry_event.set()
        # 不要立即清除，让等待的任务有机会处理
        # 清除操作将在等待任务处理完成后进行

    async def _wait_for_task_completion(self):
        """等待有任务完成（资源释放）"""
        logger.info("⏳ 等待其他任务完成释放资源...")

        # 设置超时机制，避免永久等待
        try:
            await asyncio.wait_for(self._retry_event.wait(), timeout=30.0)
            # 事件被触发后，清除它以便下次使用
            self._retry_event.clear()
        except asyncio.TimeoutError:
            logger.warning("⏰ 等待资源超时，强制继续执行")
            # 超时后强制清除事件，避免死锁
            self._retry_event.clear()
            # 检查是否有其他任务在运行，如果没有，可能是系统空闲状态
            if self.get_active_task_count() == 0:
                logger.info("💡 系统空闲，无需等待资源")

    async def _retry_monitor_loop(self):
        """重试监控循环，处理等待重试的任务"""
        while self._is_running:
            try:
                # 检查重试队列是否有任务
                if not self._retry_queue.empty():
                    # 有任务等待重试，检查是否有可用资源
                    if self.get_active_task_count() < self.max_parallel_tasks:
                        # 有可用资源，尝试处理重试队列中的任务
                        try:
                            task_info = self._retry_queue.get_nowait()
                            # 重新提交任务进行重试
                            asyncio.create_task(self._retry_task(task_info))
                        except asyncio.QueueEmpty:
                            pass
                    else:
                        # 没有可用资源，等待
                        await asyncio.sleep(0.1)
                else:
                    # 没有任务等待重试，稍作等待
                    await asyncio.sleep(0.5)

            except Exception as e:
                logger.info("重试监控器错误: %s", e)
                continue

    async def _retry_task(self, task_info: TaskInfo):
        """处理重试任务"""
        try:
            # 重新执行任务
            await self._execute_task_with_smart_retry(task_info)
        except Exception as e:
            # 重试失败，任务最终失败
            task_info.status = TaskStatus.FAILED
            task_info.error = e
            task_info.end_time = time.time()
            logger.info("💥 任务 %s 最终失败", task_info.id)
            await self._print_status()

    def _get_optimal_server(self, exclude_server: str = None) -> str:
        """选择最优服务器（考虑错误率和活跃任务数）"""
        candidates = self.available_server_ids.copy()
        if exclude_server and exclude_server in candidates:
            candidates.remove(exclude_server)

        if not candidates:
            # 如果没有候选服务器，只能使用排除的服务器
            candidates = self.available_server_ids.copy()

        # 优先选择错误率低且活跃任务少的服务器
        return min(
            candidates,
            key=lambda server: (
                self.server_error_count[server],
                self.server_active_tasks[server],
            ),
        )

    def get_active_task_count(self) -> int:
        return sum(
            1 for task in self.tasks.values() if task.status == TaskStatus.RUNNING
        )

    def get_waiting_task_count(self) -> int:
        return sum(
            1
            for task in self.tasks.values()
            if task.status == TaskStatus.WAITING_FOR_RESOURCE
        )

    def get_server_status(self) -> Dict[str, Dict]:
        return {
            server_id: {
                "total_completed": self.server_stats[server_id],
                "active_tasks": self.server_active_tasks[server_id],
                "error_count": self.server_error_count[server_id],
            }
            for server_id in self.available_server_ids
        }

    def _count_completed_tasks(self) -> int:
        """统计已完成任务数量（以任务最终状态为准）"""
        return sum(1 for t in self.tasks.values() if t.status == TaskStatus.COMPLETED)

    @staticmethod
    def _format_progress_bar(current: int, total: int, width: int = 30) -> str:
        """格式化 tqdm 风格进度条。

        - current: 已完成数量
        - total: 总数量（0 时返回空进度条）
        - width: 进度条宽度
        """
        if total <= 0:
            return "|" + "-" * width + "| 0.0% (0/0)"

        ratio = max(0.0, min(1.0, current / total))
        filled = int(round(width * ratio))
        p_bar = "|" + "█" * filled + "-" * (width - filled) + "|"
        percent = ratio * 100
        return f"{p_bar} {percent:5.1f}% ({current}/{total})"

    async def _print_status(self):
        """打印当前状态"""
        active_count = self.get_active_task_count()
        waiting_count = self.get_waiting_task_count()
        server_status = self.get_server_status()
        total_tasks = len(self.tasks)
        completed_tasks = self._count_completed_tasks()

        status_msg = "\n=== 系统状态 ==="
        status_msg += f"\n活跃任务: {active_count}, 等待重试: {waiting_count}, 最大并行: {self.max_parallel_tasks}"

        for server_id, status in server_status.items():
            status_msg += (
                f"\n服务器 {server_id}: {status['active_tasks']}活跃, "
                f"{status['total_completed']}完成, {status['error_count']}错误"
            )

        # 汇总与进度
        status_msg += f"\n总任务数: {total_tasks}"
        status_msg += f"\n总完成数: {completed_tasks}"
        status_msg += "\n进度: " + self._format_progress_bar(
            completed_tasks, total_tasks, width=30
        )
        status_msg += "\n" + "=" * 40
        logger.info(status_msg)

    def get_task_info(self, task_id: str) -> Optional[TaskInfo]:
        return self.tasks.get(task_id)
