import random
import time
from typing import Dict, Any
from task_balancer.manager import TaskQueueManager, TaskStatus
from task_balancer.utils.log_helper import logger

def simulated_task(**kwargs) -> Dict[str, Any]:
    """
    模拟异步任务函数
    """
    server_id = kwargs.get("server_id")
    data = kwargs.get("data", {})
    task_id = kwargs.get("task_id", "unknown")

    # 模拟处理时间 (1-5秒)
    process_time = random.uniform(1.0, 5.0)
    time.sleep(process_time)

    # 模拟10%的失败率
    if random.random() < 0.1:
        raise Exception(f"模拟任务失败: {task_id}")

    result = {
        "task_id": task_id,
        "server_id": server_id,
        "processed_data": f"processed_{data.get('value', 0)}",
        "process_time": process_time,
        "timestamp": time.time(),
    }

    return result


def demo_dynamic_task_addition():
    """
    演示动态添加任务的功能
    """
    logger.info("🚀 开始动态任务添加演示")

    manager = TaskQueueManager(
        task_function=simulated_task,
        server_param_name="server_id",
        available_server_ids=["dynamic_01", "dynamic_02"],
        max_parallel_tasks=2,
        max_retries=2,
    )

    try:
        manager.start()

        # 定期添加新任务
        for batch in range(5):
            tasks = [
                {"data": {"value": i, "batch": batch, "dynamic": True}}
                for i in range(2)
            ]
            task_ids = manager.submit_tasks(tasks)
            logger.info("📦 动态添加批次 %s: %s 个任务", batch, len(task_ids))
            time.sleep(2)

        logger.info("🎯 动态演示结束: 共处理 %s 个任务", len(manager.tasks))

    finally:
        manager.stop()

def demo_dynamic_task_addition_2():
    """
    演示动态添加任务的功能
    """
    logger.info("🚀 开始动态任务添加演示")

    manager = TaskQueueManager(
        task_function=simulated_task,
        server_param_name="server_id",
        available_server_ids=["dynamic_01", "dynamic_02"],
        max_parallel_tasks=20,
        max_retries=2,
        max_completed_tasks_to_keep=1
    )

    try:
        manager.start()

        def add_tasks():
            """定期添加新任务"""
            tasks = [
                {"data": {"value": i, "batch": "batch", "dynamic": True}}
                for i in range(2)
            ]
            task_ids = manager.submit_tasks(tasks)
            logger.info("📦 动态添加批次 %s: %s 个任务", "batch", len(task_ids))

        while True:
            add_tasks()
            time.sleep(1)  # 触发下一轮调度
            active_task = manager.get_active_task_count()
            logger.info("📊 当前活跃任务数: %d", active_task)
            if active_task < 0:
                add_tasks()
            if manager._completed_total > 50:
                break

    finally:
        manager.stop()


if __name__ == "__main__":
    demo_dynamic_task_addition_2()
