from typing import Any, List, Callable, Dict, Optional
import asyncio
import time
from dataclasses import dataclass
from enum import Enum
import concurrent.futures
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setLevel(logging.INFO)
    _formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    _handler.setFormatter(_formatter)
    logger.addHandler(_handler)


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
        print("🔧 任务管理器已启动")

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
        print("🔧 任务管理器已停止")

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

        print(f"📤 提交任务 {task_id}")
        await self._print_status()

        try:
            async with self._semaphore:
                result = await self._execute_task_with_smart_retry(task_info)
                return result
        except Exception as e:
            task_info.status = TaskStatus.FAILED
            task_info.error = e
            task_info.end_time = time.time()
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
                    print(f"💥 任务 {task_info.id} 最终失败")
                    await self._print_status()
                    raise e

                # 将任务加入重试队列，等待有任务完成
                task_info.status = TaskStatus.WAITING_FOR_RESOURCE
                await self._retry_queue.put(task_info)
                print(
                    f"🔄 任务 {task_info.id} 加入重试队列 (重试 {task_info.retry_count}/{self.max_retries})"
                )
                await self._print_status()

                # 等待有任务完成（资源释放）
                await self._wait_for_task_completion()

    async def _execute_single_attempt(
        self, task_info: TaskInfo, server_id: str, original_kwargs: Dict
    ) -> Any:
        """执行单次任务尝试"""
        # 更新任务状态
        task_info.status = TaskStatus.RUNNING
        task_info.server_id = server_id
        task_info.start_time = time.time()
        self.server_active_tasks[server_id] += 1

        print(
            f"🔄 任务 {task_info.id} 在服务器 {server_id} 上执行 (尝试 {task_info.retry_count + 1}/{self.max_retries + 1})"
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
            print(f"✅ 任务 {task_info.id} 完成 (耗时: {duration:.2f}s)")

            # 通知重试监控器有任务完成
            self._signal_task_completion()
            await self._print_status()

            return result

        except Exception as e:
            self.server_error_count[server_id] += 1
            print(f"❌ 任务 {task_info.id} 在服务器 {server_id} 上失败: {e}")
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
        self._retry_event.clear()  # 立即清除，以便下次等待

    async def _wait_for_task_completion(self):
        """等待有任务完成（资源释放）"""
        print("⏳ 等待其他任务完成释放资源...")
        await self._retry_event.wait()

    async def _retry_monitor_loop(self):
        """重试监控循环，处理等待重试的任务"""
        while self._is_running:
            try:
                # 检查重试队列是否有任务
                if not self._retry_queue.empty():
                    # 有任务等待重试，但需要等待有任务完成
                    await asyncio.sleep(0.1)  # 短暂等待，让主流程处理任务完成信号
                else:
                    # 没有任务等待重试，稍作等待
                    await asyncio.sleep(0.5)

            except Exception as e:
                print(f"重试监控器错误: {e}")
                continue

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

    async def _print_status(self):
        """打印当前状态"""
        active_count = self.get_active_task_count()
        waiting_count = self.get_waiting_task_count()
        server_status = self.get_server_status()

        status_msg = "\n=== 系统状态 ==="
        status_msg += f"\n活跃任务: {active_count}, 等待重试: {waiting_count}, 最大并行: {self.max_parallel_tasks}"

        for server_id, status in server_status.items():
            status_msg += (
                f"\n服务器 {server_id}: {status['active_tasks']}活跃, "
                f"{status['total_completed']}完成, {status['error_count']}错误"
            )

        status_msg += f"\n总任务数: {len(self.tasks)}"
        status_msg += "\n" + "=" * 40
        print(status_msg)

    def get_task_info(self, task_id: str) -> Optional[TaskInfo]:
        return self.tasks.get(task_id)
