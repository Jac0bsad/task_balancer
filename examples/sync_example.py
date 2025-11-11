import asyncio
import random
import time
from task_balancer.manager import AsyncTaskQueueManager


def mock_sync_task(server_id: str, task_data: str, delay: float = 1.0) -> str:
    """
    模拟同步任务，有一定概率失败
    """
    # 模拟处理时间
    time.sleep(delay)

    # 15% 概率失败
    if random.random() < 0.15:
        raise Exception(f"模拟同步任务在服务器 {server_id} 上失败")

    return f"同步任务 '{task_data}' 在服务器 {server_id} 上成功完成"


async def sync_task_example():
    """
    同步任务使用示例
    """
    print("\n🔄 开始同步任务示例")

    manager = AsyncTaskQueueManager(
        task_function=mock_sync_task,
        server_param_name="server_id",
        available_server_ids=["server_A", "server_B"],
        max_parallel_tasks=10,
        max_retries=1,
    )

    # 启动管理器
    await manager.start()

    try:
        # 准备任务列表
        tasks = [{"task_data": f"同步任务_{i}", "delay": 2} for i in range(50)]

        # 并行提交所有任务
        async_tasks = [
            manager.submit_single_task(task_args, f"task_{i+1}")
            for i, task_args in enumerate(tasks)
        ]

        results = await asyncio.gather(*async_tasks, return_exceptions=True)

        # 处理结果
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"❌ 任务 {i+1} 失败: {result}")
            else:
                print(f"✅ 任务 {i+1} 成功: {result}")

    finally:
        await manager.stop()


async def main():
    await sync_task_example()


if __name__ == "__main__":
    asyncio.run(main())
