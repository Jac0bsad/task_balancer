from typing import Any, List, Callable, Dict, Optional, Set
from collections import deque
import time
import threading
from dataclasses import dataclass
from enum import Enum
import concurrent.futures
import asyncio
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
    future: Optional[concurrent.futures.Future] = None


class TaskQueueManager:
    def __init__(
        self,
        task_function: Callable,
        server_param_name: str,
        available_server_ids: List[Any],
        max_parallel_tasks: int = 20,
        max_retries: int = 3,
        max_completed_tasks_to_keep: Optional[int] = None,
    ):
        """
        Args:
            max_completed_tasks_to_keep: 最大保留的“已完成任务”数量（仅成功的）。None 表示不限制；<=0 表示不保留。
                如果不限制，任务信息会随着时间累积，占用更多内存。
        """
        self.task_function = task_function
        self.server_param_name = server_param_name
        self.available_server_ids = list(available_server_ids)
        self.max_parallel_tasks = max_parallel_tasks
        self.max_retries = max_retries
        self.max_completed_tasks_to_keep = max_completed_tasks_to_keep

        # 任务管理
        self.tasks: Dict[str, TaskInfo] = {}
        self._task_id_counter = 0
        self._pending_tasks: Set[str] = set()  # 待处理任务ID集合

        # 服务器状态
        self.server_stats = {server_id: 0 for server_id in self.available_server_ids}
        self.server_active_tasks = {
            server_id: 0 for server_id in self.available_server_ids
        }
        self.server_error_count = {
            server_id: 0 for server_id in self.available_server_ids
        }

        # 控制
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_parallel_tasks
        )
        self._is_running = False
        self._lock = threading.RLock()
        self._pbar_lock = threading.Lock()

        # tqdm 相关
        self._pbar = None
        # 记录完成顺序的任务ID（仅成功任务）
        self._completed_task_ids = deque()
        # 全局进度统计（避免被清理影响展示）
        self._total_submitted = 0
        self._completed_total = 0
        self._failed_total = 0
        self._finished_total = 0

    def start(self) -> None:
        """启动任务管理器（同步）。"""
        with self._lock:
            if self._is_running:
                return
            self._is_running = True

            # 初始化 tqdm 进度条
            self._pbar = tqdm(
                total=self._total_submitted,
                unit="task",
                dynamic_ncols=True,
                desc="Tasks",
                leave=True,
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_inv_fmt}] {postfix}",
            )

        logger.info("🔧 任务管理器已启动")

    def stop(self) -> None:
        """停止管理器（同步）。"""
        with self._lock:
            if not self._is_running:
                return
            self._is_running = False

        # 等待所有任务完成
        self._wait_until_finished(timeout=None)

        # 最终刷新 tqdm
        with self._pbar_lock:
            if self._pbar is not None:
                total = self._total_submitted
                finished = self._finished_total
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
        self._executor.shutdown(wait=True)

        # 关闭 tqdm 进度条
        with self._pbar_lock:
            if self._pbar is not None:
                try:
                    self._pbar.close()
                finally:
                    self._pbar = None

        logger.info("🔧 任务管理器已停止")

    def submit_tasks(self, tasks_kwargs: List[Dict[str, Any]]) -> List[str]:
        """
        提交多个任务，任务会自动开始运行
        返回任务ID列表（同步接口）
        """
        with self._lock:
            if not self._is_running:
                raise RuntimeError("任务管理器未启动，请先调用 start() 方法")

            task_ids = []
            for kwargs in tasks_kwargs:
                self._task_id_counter += 1
                task_id = f"task_{self._task_id_counter}"

                task_info = TaskInfo(
                    id=task_id, kwargs=kwargs, status=TaskStatus.PENDING
                )
                self.tasks[task_id] = task_info
                self._pending_tasks.add(task_id)
                task_ids.append(task_id)

            # 统计累计提交数量
            self._total_submitted += len(tasks_kwargs)

            # 更新 tqdm 总量
            with self._pbar_lock:
                if self._pbar is not None:
                    self._pbar.total = self._total_submitted
                    self._pbar.refresh()

        logger.info(
            "📤 提交 %d 个任务，总任务数: %d", len(tasks_kwargs), self._total_submitted
        )
        self._print_status()

        # 立即调度任务执行
        for task_id in task_ids:
            task_info = self.tasks[task_id]
            future = self._executor.submit(
                self._execute_task_with_smart_retry, task_info
            )
            task_info.future = future

        return task_ids

    def submit_single_task(self, task_kwargs: Dict[str, Any]) -> str:
        """
        提交单个任务，任务会自动开始运行
        返回任务ID（同步接口）
        """
        return self.submit_tasks([task_kwargs])[0]

    def wait_for_completion(self, timeout: Optional[float] = None) -> bool:
        """
        等待所有任务完成（同步）。
        返回是否所有任务都已完成
        """
        with self._lock:
            if not self._is_running:
                raise RuntimeError("任务管理器未启动")

        start_time = time.time()
        while True:
            # 检查是否所有任务都已完成
            with self._lock:
                finished_ok = (
                    self._finished_total >= self._total_submitted
                    and self.get_active_task_count() == 0
                )
            if finished_ok:
                return True

            # 检查超时
            if timeout is not None and time.time() - start_time > timeout:
                logger.warning("⏰ 等待任务完成超时")
                return False

            # 等待一段时间再检查
            time.sleep(0.1)

    def get_task_result(self, task_id: str) -> Any:
        """获取任务结果，如果任务未完成会等待（同步）。"""
        with self._lock:
            if task_id not in self.tasks:
                raise ValueError(f"任务ID {task_id} 不存在或已被清理")
            task_info = self.tasks[task_id]

        # 如果任务有future，等待它完成
        if task_info.future is not None:
            task_info.future.result()  # 阻塞直到完成或抛出异常

        with self._lock:
            if task_info.status == TaskStatus.COMPLETED:
                return task_info.result
            elif task_info.status == TaskStatus.FAILED:
                # 重新抛出原始异常
                if task_info.error:
                    raise task_info.error
                raise RuntimeError("任务失败，但未记录错误详情")
            else:
                raise RuntimeError(f"任务 {task_id} 状态异常: {task_info.status}")

    def _execute_task_with_smart_retry(self, task_info: TaskInfo) -> Any:
        """执行任务，使用智能重试策略（在线程池工作线程中运行）。"""
        original_kwargs = task_info.kwargs.copy()

        while True:
            with self._lock:
                retry_allowed = task_info.retry_count <= self.max_retries
            if not retry_allowed:
                # 正常不会到这里，失败逻辑在异常分支中处理
                return None

            try:
                # 选择最优服务器（避开最近失败的服务器）
                with self._lock:
                    server_id = self._get_optimal_server(task_info.last_failed_server)

                # 执行任务
                return self._execute_single_attempt(
                    task_info, server_id, original_kwargs
                )

            except Exception as e:
                with self._lock:
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
                        # 更新最终失败统计
                        self._on_task_failed(task_info)
                        logger.info("💥 任务 %s 最终失败", task_info.id)
                        self._print_status()
                        raise

                # 优先在其他服务器上立刻重试，避免长时间等待
                with self._lock:
                    other_servers_exist = any(
                        s != task_info.last_failed_server
                        for s in self.available_server_ids
                    )
                if other_servers_exist:
                    logger.info(
                        "🔁 任务 %s 切换服务器重试 (第 %d/%d 次)",
                        task_info.id,
                        task_info.retry_count,
                        self.max_retries,
                    )
                    time.sleep(0.05)
                    continue
                else:
                    # 仅有单台服务器时，做一点退避再重试
                    time.sleep(min(0.5, 0.1 * task_info.retry_count))
                    continue

    def _execute_single_attempt(
        self, task_info: TaskInfo, server_id: str, original_kwargs: Dict
    ) -> Any:
        """执行单次任务尝试（在线程池工作线程中运行）。"""
        # 更新任务状态
        with self._lock:
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
        self._print_status()

        try:
            # 执行任务
            task_kwargs = original_kwargs.copy()
            task_kwargs[self.server_param_name] = server_id

            if asyncio.iscoroutinefunction(self.task_function):
                # 在工作线程内创建并运行事件循环
                result = asyncio.run(self.task_function(**task_kwargs))
            else:
                result = self.task_function(**task_kwargs)

            # 任务成功完成
            end_time = time.time()
            with self._lock:
                task_info.status = TaskStatus.COMPLETED
                task_info.result = result
                task_info.end_time = end_time
                self.server_stats[server_id] += 1

                # 标记完成并按配置清理多余的已完成任务
                self._on_task_completed(task_info)
                self._completed_total += 1
                self._finished_total += 1

            duration = end_time - (task_info.start_time or end_time)
            logger.info("✅ 任务 %s 完成 (耗时: %.2fs)", task_info.id, duration)

            self._print_status()

            return result

        except Exception as e:
            with self._lock:
                self.server_error_count[server_id] += 1
            logger.info("❌ 任务 %s 在服务器 %s 上失败: %s", task_info.id, server_id, e)
            raise

        finally:
            # 清理活跃任务计数
            with self._lock:
                if server_id in self.server_active_tasks:
                    self.server_active_tasks[server_id] = max(
                        0, self.server_active_tasks[server_id] - 1
                    )

    def _count_completed_tasks(self) -> int:
        """统计已完成任务数量（以任务最终状态为准）"""
        with self._lock:
            return sum(
                1 for t in self.tasks.values() if t.status == TaskStatus.COMPLETED
            )

    def _count_finished_tasks(self) -> int:
        """统计已结束任务数量（成功或失败）"""
        with self._lock:
            return sum(
                1
                for t in self.tasks.values()
                if t.status in (TaskStatus.COMPLETED, TaskStatus.FAILED)
            )

    def _print_status(self) -> None:
        """打印当前各服务器状态（同步）。"""
        with self._lock:
            active_count = self.get_active_task_count()
            server_status = self.get_server_status()
            total_submitted = self._total_submitted
            finished_total = self._finished_total

        # 用"已结束=完成+失败"驱动 tqdm（使用全局计数，避免清理影响）
        with self._pbar_lock:
            if self._pbar is not None:
                self._pbar.total = total_submitted
                self._pbar.n = finished_total
                self._pbar.set_postfix({"running": active_count})
                self._pbar.refresh()

        # 文本状态（不包含进度条）
        status_lines = [
            "\n=== 系统状态 ===",
            f"活跃任务: {active_count}, 最大并行: {self.max_parallel_tasks}",
            f"待处理任务: {len(self._pending_tasks)}",
        ]
        for server_id, status in server_status.items():
            status_lines.append(
                f"服务器 {server_id}: {status['active_tasks']}活跃, {status['total_completed']}完成, {status['error_count']}错误"
            )
        status_lines.append(f"总任务数: {total_submitted}")
        with self._lock:
            status_lines.append(
                f"总完成数: {self._completed_total} (失败: {self._failed_total})"
            )
        status_lines.append("".ljust(40, "="))
        logger.info("\n".join(status_lines))

    def _wait_until_finished(self, timeout: Optional[float] = None) -> None:
        """等待所有任务进入终态（完成或失败），或超时（同步）。"""
        if timeout is None:
            while True:
                with self._lock:
                    done = (
                        self._finished_total >= self._total_submitted
                        and self.get_active_task_count() == 0
                    )
                if done:
                    break
                time.sleep(0.1)
        else:
            end = time.time() + timeout
            while time.time() < end:
                with self._lock:
                    done = (
                        self._finished_total >= self._total_submitted
                        and self.get_active_task_count() == 0
                    )
                if done:
                    break
                time.sleep(0.1)

    def _get_optimal_server(self, exclude_server: str | None = None) -> str:
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

    def _on_task_completed(self, task_info: TaskInfo) -> None:
        """记录任务完成并执行必要的清理。仅针对成功任务。"""
        self._completed_task_ids.append(task_info.id)
        self._cleanup_completed_tasks()

    def _cleanup_completed_tasks(self) -> None:
        """清理超过上限的已完成任务（仅成功）。"""
        limit = self.max_completed_tasks_to_keep
        if limit is None:
            return  # 不限制
        # 将负数视为 0：不保留任何已完成任务
        try:
            keep = int(limit)
        except Exception:
            # 非法值时，忽略清理以避免误删
            return
        if keep < 0:
            keep = 0

        while len(self._completed_task_ids) > keep:
            old_id = self._completed_task_ids.popleft()
            old_info = self.tasks.get(old_id)
            # 仅删除仍为 COMPLETED 的任务
            if old_info and old_info.status == TaskStatus.COMPLETED:
                try:
                    del self.tasks[old_id]
                except KeyError:
                    pass

    def _on_task_failed(self, _: TaskInfo) -> None:
        """记录任务最终失败（仅在最终失败时调用）。"""
        self._failed_total += 1
        self._finished_total += 1

    def get_active_task_count(self) -> int:
        with self._lock:
            return sum(
                1 for task in self.tasks.values() if task.status == TaskStatus.RUNNING
            )

    def get_server_status(self) -> Dict[str, Dict]:
        with self._lock:
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
        with self._lock:
            if task_id not in self.tasks:
                raise ValueError(f"任务ID {task_id} 不存在或已被清理")
            return self.tasks[task_id].status

    def has_pending_tasks(self) -> bool:
        """检查是否有待处理任务"""
        with self._lock:
            return any(t.status == TaskStatus.PENDING for t in self.tasks.values())

    def wait_for_idle_server(self, timeout: Optional[float] = None) -> int:
        """
        等待直到存在空闲服务器（当前运行任务数 < max_parallel_tasks）。
        返回可用的任务名额数量（>=1）。如超时则返回 0。
        """
        with self._lock:
            if not self._is_running:
                raise RuntimeError("任务管理器未启动")

        start = time.time()
        while True:
            active = self.get_active_task_count()
            if active < self.max_parallel_tasks:
                return self.max_parallel_tasks - active

            if timeout is not None and (time.time() - start) > timeout:
                logger.warning("⏰ 等待空闲服务器超时")
                return 0

            time.sleep(1)
