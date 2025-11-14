import random
import time
from typing import Dict, Any
from task_balancer.manager import TaskQueueManager
from task_balancer.utils.log_helper import logger


def simulated_sync_task(**kwargs) -> Dict[str, Any]:
    """
    模拟同步任务函数
    """
    server_id = kwargs.get("server_id")
    data = kwargs.get("data", {})
    task_id = kwargs.get("task_id", "unknown")

    # 模拟处理时间 (1-3秒)
    process_time = random.uniform(1.0, 3.0)
    time.sleep(process_time)

    # 模拟5%的失败率
    if random.random() < 0.05:
        raise Exception(f"模拟同步任务失败: {task_id}")

    result = {
        "task_id": task_id,
        "server_id": server_id,
        "processed_data": f"sync_processed_{data.get('value', 0)}",
        "process_time": process_time,
        "timestamp": time.time(),
    }

    return result


def demo_sync_tasks():
    """
    演示同步任务管理器的使用
    """
    logger.info("🚀 开始同步任务管理器演示")

    manager = TaskQueueManager(
        task_function=simulated_sync_task,
        server_param_name="server_id",
        available_server_ids=["sync_server_01", "sync_server_02"],
        max_parallel_tasks=2,
        max_retries=1,
    )

    try:
        manager.start()

        # 提交同步任务
        sync_tasks = [{"data": {"value": i, "type": "sync"}} for i in range(4)]
        task_ids = manager.submit_tasks(sync_tasks)

        logger.info("✅ 提交 %d 个同步任务", len(task_ids))

        # 等待完成
        manager.wait_for_completion(timeout=10.0)

        # 显示结果
        for task_id in task_ids:
            status = manager.get_task_status(task_id)
            logger.info("同步任务 %s: %s", task_id, status.value)

    finally:
        manager.stop()
        logger.info("🛑 同步任务演示结束")


if __name__ == "__main__":
    demo_sync_tasks()
