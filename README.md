# task_balancer

自动分配一批任务，用于有并发限制的api调用等，通过开多个账号，然后分配任务，提高并发量

## 注意

1. 通过线程池分配任务
2. 所有的任务信息（参数、结果）默认会累积保存在 `TaskQueueManager.tasks` 中
    - 若在运行过程中添加新的任务，内存也会累积增加
    - 可通过 max_completed_tasks_to_keep 参数限制保存的任务信息数量
    - 若执行的任务不关心返回值，建议将 max_completed_tasks_to_keep 设置为负数节省内存
    - 若某个已完成任务被清理，`get_task_status(task_id)`/`get_task_result(task_id)` 将抛出“任务不存在或已被清理”的异常；如需长期保存结果，请在完成后及时取出后自行持久化。

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
