from typing import Any, List, Callable, Dict, Optional
import asyncio
import time
from dataclasses import dataclass
from enum import Enum
import concurrent.futures
from tqdm import tqdm
from task_balancer.utils.log_helper import logger


class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


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

        # 服务器状态
        self.server_stats = {server_id: 0 for server_id in available_server_ids}
        self.server_active_tasks = {server_id: 0 for server_id in available_server_ids}
        self.server_error_count = {server_id: 0 for server_id in available_server_ids}

        # 执行器和控制
        self._semaphore = asyncio.Semaphore(max_parallel_tasks)
        self._thread_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_parallel_tasks
        )
        self._is_running = False

        # tqdm 相关
        self._pbar = None  # type: ignore
        self._start_time: Optional[float] = None

    async def start(self):
        """启动任务管理器"""
        if self._is_running:
            return

        self._is_running = True
        self._start_time = time.time()
        # 始终初始化 tqdm 进度条
        self._pbar = tqdm(
            total=len(self.tasks),
            unit="task",
            dynamic_ncols=True,
            desc="Tasks",
            leave=True,
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_inv_fmt}] {postfix}",
        )
        logger.info("🔧 任务管理器已启动")

    async def stop(self):
        """停止管理器"""
        if not self._is_running:
            return

        self._is_running = False

        # 等待短时间以便重试任务收敛（避免未完成导致进度非100%）
        await self._wait_until_finished(timeout=60.0)

        # 最终刷新 tqdm 至完成（按“已结束=成功或失败”计数）
        if self._pbar is not None:
            total = len(self.tasks)
            finished = self._count_finished_tasks()
            active_count = self.get_active_task_count()
            self._pbar.total = total
            self._pbar.n = finished
            self._pbar.set_postfix({"running": active_count})
            self._pbar.refresh()

        # 输出最终状态统计
        server_status = self.get_server_status()
        summary_lines = ["📊 最终状态统计:"]
        for server_id in self.available_server_ids:
            s = server_status[server_id]
            summary_lines.append(
                f"  服务器 {server_id}: {s['total_completed']} 完成, {s['error_count']} 错误, {s['active_tasks']} 活跃"
            )
        logger.info("\n".join(summary_lines))

        # 关闭线程池
        self._thread_pool.shutdown(wait=True)

        # 关闭 tqdm 进度条
        if self._pbar is not None:
            try:
                self._pbar.close()
            finally:
                self._pbar = None

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

        # 新任务加入后，更新 tqdm 总量
        self._pbar.total = len(self.tasks)
        self._pbar.refresh()

        logger.info("📤 提交任务 %s", task_id)
        await self._print_status()

        # 使用上下文管理信号量，确保自动释放
        async with self._semaphore:
            return await self._execute_task_with_smart_retry(task_info)

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
                    # 统计错误次数（若 server_id 已有值）
                    if task_info.server_id in self.server_error_count:
                        self.server_error_count[task_info.server_id] += 1
                    logger.info("💥 任务 %s 最终失败", task_info.id)
                    await self._print_status()
                    raise e

                # 优先在其他服务器上立刻重试，避免长时间等待
                other_servers = [
                    s
                    for s in self.available_server_ids
                    if s != task_info.last_failed_server
                ]
                if other_servers:
                    logger.info(
                        "🔁 任务 %s 切换服务器重试 (第 %d/%d 次)",
                        task_info.id,
                        task_info.retry_count,
                        self.max_retries,
                    )
                    # 轻微退避，给事件循环机会处理其他任务
                    await asyncio.sleep(0.05)
                    continue
                else:
                    # 仅有单台服务器时，做一点退避再重试
                    await asyncio.sleep(min(0.5, 0.1 * task_info.retry_count))
                    continue

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
                loop = asyncio.get_running_loop()
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

    def _count_completed_tasks(self) -> int:
        """统计已完成任务数量（以任务最终状态为准）"""
        return sum(1 for t in self.tasks.values() if t.status == TaskStatus.COMPLETED)

    def _count_finished_tasks(self) -> int:
        """统计已结束任务数量（成功或失败）"""
        return sum(
            1
            for t in self.tasks.values()
            if t.status in (TaskStatus.COMPLETED, TaskStatus.FAILED)
        )

    async def _print_status(self):
        """打印当前各服务器状态"""
        active_count = self.get_active_task_count()
        server_status = self.get_server_status()
        total_tasks = len(self.tasks)
        completed_tasks = self._count_completed_tasks()
        finished_tasks = self._count_finished_tasks()

        # 用“已结束=完成+失败”驱动 tqdm
        self._pbar.total = total_tasks
        self._pbar.n = finished_tasks

        # 保留运行中/等待数
        self._pbar.set_postfix({"running": active_count})
        self._pbar.refresh()

        # 文本状态（不包含进度条）
        status_msg = "\n=== 系统状态 ==="
        status_msg += f"\n活跃任务: {active_count}, 最大并行: {self.max_parallel_tasks}"
        for server_id, status in server_status.items():
            status_msg += (
                f"\n服务器 {server_id}: {status['active_tasks']}活跃, "
                f"{status['total_completed']}完成, {status['error_count']}错误"
            )
        status_msg += f"\n总任务数: {total_tasks}"
        status_msg += f"\n总完成数: {completed_tasks}"
        status_msg += "\n" + "=" * 40
        logger.info(status_msg)

    async def _wait_until_finished(self, timeout: float = 60.0) -> None:
        """等待所有任务进入终态（完成或失败），或超时"""
        end = time.time() + timeout
        while time.time() < end:
            if (
                self._count_finished_tasks() >= len(self.tasks)
                and self.get_active_task_count() == 0
            ):
                break
            await asyncio.sleep(0.1)

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

    def get_server_status(self) -> Dict[str, Dict]:
        return {
            server_id: {
                "total_completed": self.server_stats[server_id],
                "active_tasks": self.server_active_tasks[server_id],
                "error_count": self.server_error_count[server_id],
            }
            for server_id in self.available_server_ids
        }
