# task_balancer

通过队列自动分配任务，用于有并发限制的api调用等，通过开多个账号，然后分配任务，提高并发量

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
