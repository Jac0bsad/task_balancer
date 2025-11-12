import asyncio
import random
import time
from typing import Dict, Any
from task_balancer.manager import AsyncTaskQueueManager, TaskStatus
from task_balancer.utils.log_helper import logger


async def simulated_async_task(**kwargs) -> Dict[str, Any]:
    """
    模拟异步任务函数
    """
    server_id = kwargs.get("server_id")
    data = kwargs.get("data", {})
    task_id = kwargs.get("task_id", "unknown")

    # 模拟处理时间 (1-5秒)
    process_time = random.uniform(1.0, 5.0)

    # 模拟10%的失败率
    if random.random() < 0.1:
        await asyncio.sleep(process_time)
        raise Exception(f"模拟任务失败: {task_id}")

    await asyncio.sleep(process_time)

    result = {
        "task_id": task_id,
        "server_id": server_id,
        "processed_data": f"processed_{data.get('value', 0)}",
        "process_time": process_time,
        "timestamp": time.time(),
    }

    return result


async def demo_dynamic_task_addition():
    """
    演示动态添加任务的功能
    """
    logger.info("🚀 开始动态任务添加演示")

    manager = AsyncTaskQueueManager(
        task_function=simulated_async_task,
        server_param_name="server_id",
        available_server_ids=["dynamic_01", "dynamic_02"],
        max_parallel_tasks=2,
        max_retries=2,
    )

    try:
        await manager.start()

        async def add_tasks_periodically():
            """定期添加新任务"""
            for batch in range(5):
                tasks = [
                    {"data": {"value": i, "batch": batch, "dynamic": True}}
                    for i in range(2)
                ]
                task_ids = await manager.submit_tasks(tasks)
                logger.info("📦 动态添加批次 %s: %s 个任务", batch, len(task_ids))
                await asyncio.sleep(20)  # 每2秒添加一批
                # 注意：submit一个task后，必须asyncio.sleep一下，退出事件循环，否则无法触发任务调度

        # 启动动态添加任务
        add_task = asyncio.create_task(add_tasks_periodically())

        # 同时监控任务状态
        # start_time = time.time()
        # while time.time() - start_time < 15:  # 监控15秒
        #     active_count = manager.get_active_task_count()
        #     pending_count = (
        #         len(manager._pending_tasks) if hasattr(manager, "_pending_tasks") else 0
        #     )
        #     total_tasks = len(manager.tasks)

        #     logger.info(
        #         f"📊 实时状态: {active_count} 活跃, {pending_count} 待处理, {total_tasks} 总任务"
        #     )
        #     await asyncio.sleep(2)

        # 取消动态添加任务
        # add_task.cancel()
        try:
            await add_task
        except asyncio.CancelledError:
            pass

        logger.info("🎯 动态演示结束: 共处理 %s 个任务", len(manager.tasks))

    finally:
        await manager.stop()


async def demo_dynamic_task_addition_2():
    """
    演示动态添加任务的功能
    """
    logger.info("🚀 开始动态任务添加演示")

    manager = AsyncTaskQueueManager(
        task_function=simulated_async_task,
        server_param_name="server_id",
        available_server_ids=["dynamic_01", "dynamic_02"],
        max_parallel_tasks=20,
        max_retries=2,
        max_completed_tasks_to_keep=1
    )

    try:
        await manager.start()

        async def add_tasks():
            """定期添加新任务"""
            tasks = [
                {"data": {"value": i, "batch": "batch", "dynamic": True}}
                for i in range(2)
            ]
            task_ids = await manager.submit_tasks(tasks)
            logger.info("📦 动态添加批次 %s: %s 个任务", "batch", len(task_ids))

        while True:
            await add_tasks()
            await asyncio.sleep(1)  # 退出事件循环，触发任务调度
            active_task = manager.get_active_task_count()
            logger.info("📊 当前活跃任务数: %d", active_task)
            if active_task < 0:
                await add_tasks()
            if manager._completed_total > 50:
                break

    finally:
        await manager.stop()


if __name__ == "__main__":
    asyncio.run(demo_dynamic_task_addition_2())
