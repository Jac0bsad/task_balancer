# task_balancer

通过队列自动分配任务，用于有并发限制的api调用等，通过开多个账号，然后分配任务，提高并发量

## 注意

1. 如果函数为async异步函数，需要保证其内部是支持并发的，否则会被阻塞为所有任务同步执行
2. 提交任务后，应该asyncio.sleep()一段时间，让出控制权，给出执行任务的机会
3. 所有的任务信息（参数、结果）默认会累积保存在 `AsyncTaskQueueManager.tasks` 中，可通过参数控制清理策略，防止内存增长

## 任务保留与进度条

- `max_completed_tasks_to_keep`: 控制最多保留多少个“已完成(成功)”任务在内存中。
  - 传入 `None`（默认）表示不限制。
  - 传入 `0` 表示不保留任何成功任务（完成后立即清理）。
  - 传入正整数表示仅保留最近的 N 个成功任务，其余会被清理出 `manager.tasks`。
- 进度条统计使用全局计数（累计提交/结束），不会因为清理 `tasks` 而改变总数，保证显示稳定正确。

示例：

```python
from task_balancer.manager import AsyncTaskQueueManager

async def work(x, server_id):
    return x * 2

manager = AsyncTaskQueueManager(
    task_function=work,
    server_param_name="server_id",
    available_server_ids=["A", "B", "C"],
    max_parallel_tasks=10,
    max_retries=2,
    max_completed_tasks_to_keep=100,  # 仅保留最近 100 个成功任务
)
```

注意：若某个已完成任务被清理，`get_task_status(task_id)`/`get_task_result(task_id)` 将抛出“任务不存在或已被清理”的异常；如需长期保存结果，请在完成后及时取出后自行持久化。

## 日志使用

- 统一从 `task_balancer.utils.log_helper` 导入 `logger`：

```python
from task_balancer.utils.log_helper import logger

logger.info("hello")
```

- 可通过环境变量控制：
  - `TASK_BALANCER_LOG_LEVEL`（默认 `INFO`）
  - `TASK_BALANCER_LOG_FORMAT`（默认 `%(asctime)s [%(levelname)s] %(name)s: %(message)s`）
  - `TASK_BALANCER_LOG_DATEFMT`（默认 `%Y-%m-%d %H:%M:%S`）
  - `TASK_BALANCER_LOG_FILE`（若设置则同时写入该文件）
  - `TASK_BALANCER_LOG_FILE_MODE`（默认 `a`）
