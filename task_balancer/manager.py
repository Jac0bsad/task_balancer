from typing import Any, List, Callable, Dict, Optional, Set
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
    future: Optional[asyncio.Future] = None


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
        self._pending_tasks: Set[str] = set()  # 待处理任务ID集合

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
        self._task_runner_task: Optional[asyncio.Task] = None

        # tqdm 相关
        self._pbar = None  # type: ignore
        self._start_time: Optional[float] = None

    async def start(self):
        """启动任务管理器"""
        if self._is_running:
            return

        self._is_running = True
        self._start_time = time.time()

        # 初始化 tqdm 进度条
        self._pbar = tqdm(
            total=len(self.tasks),
            unit="task",
            dynamic_ncols=True,
            desc="Tasks",
            leave=True,
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_inv_fmt}] {postfix}",
        )

        # 启动任务运行器
        self._task_runner_task = asyncio.create_task(self._task_runner())

        logger.info("🔧 任务管理器已启动")

    async def stop(self):
        """停止管理器"""
        if not self._is_running:
            return

        self._is_running = False

        # 取消任务运行器
        if self._task_runner_task:
            self._task_runner_task.cancel()
            try:
                await self._task_runner_task
            except asyncio.CancelledError:
                pass

        # 等待所有任务完成或超时
        await self._wait_until_finished(timeout=60.0)

        # 最终刷新 tqdm
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

    async def submit_tasks(self, tasks_kwargs: List[Dict[str, Any]]) -> List[str]:
        """
        提交多个任务，任务会自动开始运行
        返回任务ID列表
        """
        if not self._is_running:
            raise RuntimeError("任务管理器未启动，请先调用 start() 方法")

        task_ids = []
        for kwargs in tasks_kwargs:
            self._task_id_counter += 1
            task_id = f"task_{self._task_id_counter}"

            task_info = TaskInfo(id=task_id, kwargs=kwargs, status=TaskStatus.PENDING)
            self.tasks[task_id] = task_info
            self._pending_tasks.add(task_id)
            task_ids.append(task_id)

        # 更新 tqdm 总量
        if self._pbar is not None:
            self._pbar.total = len(self.tasks)
            self._pbar.refresh()

        logger.info(
            "📤 提交 %d 个任务，总任务数: %d", len(tasks_kwargs), len(self.tasks)
        )
        await self._print_status()

        return task_ids

    async def submit_single_task(self, task_kwargs: Dict[str, Any]) -> str:
        """
        提交单个任务，任务会自动开始运行
        返回任务ID
        """
        task_ids = await self.submit_tasks([task_kwargs])
        return task_ids[0]

    async def wait_for_completion(self, timeout: Optional[float] = None) -> bool:
        """
        等待所有任务完成
        返回是否所有任务都已完成
        """
        if not self._is_running:
            raise RuntimeError("任务管理器未启动")

        start_time = time.time()
        while True:
            # 检查是否所有任务都已完成
            if self._count_finished_tasks() == len(self.tasks):
                return True

            # 检查超时
            if timeout is not None and time.time() - start_time > timeout:
                logger.warning("⏰ 等待任务完成超时")
                return False

            # 等待一段时间再检查
            await asyncio.sleep(0.1)

    async def get_task_result(self, task_id: str) -> Any:
        """获取任务结果，如果任务未完成会等待"""
        if task_id not in self.tasks:
            raise ValueError(f"任务ID {task_id} 不存在")

        task_info = self.tasks[task_id]

        # 如果任务有future，等待它完成
        if task_info.future and not task_info.future.done():
            await task_info.future

        if task_info.status == TaskStatus.COMPLETED:
            return task_info.result
        elif task_info.status == TaskStatus.FAILED:
            raise task_info.error
        else:
            raise RuntimeError(f"任务 {task_id} 状态异常: {task_info.status}")

    async def _task_runner(self):
        """任务运行器，持续运行处理待处理任务"""
        while self._is_running:
            try:
                # 获取待处理任务
                pending_tasks_copy = self._pending_tasks.copy()

                if not pending_tasks_copy:
                    # 没有待处理任务，短暂休眠
                    await asyncio.sleep(0.1)
                    continue

                # 处理待处理任务
                for task_id in pending_tasks_copy:
                    if not self._is_running:
                        break

                    task_info = self.tasks[task_id]
                    if task_info.status == TaskStatus.PENDING:
                        # 创建异步任务执行
                        task_info.future = asyncio.create_task(
                            self._execute_task_with_smart_retry(task_info)
                        )
                        self._pending_tasks.remove(task_id)

                await asyncio.sleep(0.01)  # 短暂让出控制权

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("任务运行器异常: %s", e)
                await asyncio.sleep(0.1)

    async def _execute_task_with_smart_retry(self, task_info: TaskInfo) -> Any:
        """执行任务，使用智能重试策略"""
        original_kwargs = task_info.kwargs.copy()

        async with self._semaphore:
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

        # 用"已结束=完成+失败"驱动 tqdm
        if self._pbar is not None:
            self._pbar.total = total_tasks
            self._pbar.n = finished_tasks
            self._pbar.set_postfix({"running": active_count})
            self._pbar.refresh()

        # 文本状态（不包含进度条）
        status_msg = "\n=== 系统状态 ==="
        status_msg += f"\n活跃任务: {active_count}, 最大并行: {self.max_parallel_tasks}"
        status_msg += f"\n待处理任务: {len(self._pending_tasks)}"
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

    def get_task_status(self, task_id: str) -> TaskStatus:
        """获取任务状态"""
        if task_id not in self.tasks:
            raise ValueError(f"任务ID {task_id} 不存在")
        return self.tasks[task_id].status

    def has_pending_tasks(self) -> bool:
        """检查是否有待处理任务"""
        return len(self._pending_tasks) > 0
