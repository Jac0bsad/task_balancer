import asyncio
import random
import time
from task_balancer.manager import AsyncTaskQueueManager


async def mock_async_task(server_id: str, task_data: str, delay: float = 1.0) -> str:
    """
    模拟异步任务，有一定概率失败
    """
    # 模拟网络延迟
    await asyncio.sleep(delay)

    # 10% 概率失败
    if random.random() < 0.1:
        raise Exception(f"模拟任务在服务器 {server_id} 上失败")

    return f"任务 '{task_data}' 在服务器 {server_id} 上成功完成"


async def basic_usage_example():
    """
    基础使用示例：演示如何创建和管理任务队列
    """
    print("🚀 开始基础使用示例")

    # 创建任务管理器
    manager = AsyncTaskQueueManager(
        task_function=mock_async_task,
        server_param_name="server_id",
        available_server_ids=["server_1", "server_2", "server_3", "server_4"],
        max_parallel_tasks=3,  # 限制最大并行任务数
        max_retries=2,  # 最大重试次数
    )

    # 启动管理器
    await manager.start()

    # 创建一批测试任务
    tasks = [
        {"task_data": f"任务_{i}", "delay": random.uniform(0.5, 2.0)} for i in range(10)
    ]

    print(f"📤 提交 {len(tasks)} 个任务...")

    # 提交任务并收集结果
    results = []
    for i, task_kwargs in enumerate(tasks):
        try:
            result = await manager.submit_single_task(
                task_kwargs=task_kwargs, task_id=f"custom_id_{i}"  # 可选：自定义任务ID
            )
            results.append(result)
            print(f"✅ 任务 {i} 完成: {result}")
        except Exception as e:
            print(f"❌ 任务 {i} 失败: {e}")

    # 显示最终状态
    print("\n📊 最终状态统计:")
    server_status = manager.get_server_status()
    for server_id, status in server_status.items():
        print(
            f"  服务器 {server_id}: {status['total_completed']} 完成, "
            f"{status['error_count']} 错误, {status['active_tasks']} 活跃"
        )

    # 停止管理器
    await manager.stop()
    print("🏁 基础使用示例完成")


async def main():
    """
    主函数：运行所有示例
    """
    print("=" * 60)
    print("🎯 Task Balancer 基础使用示例")
    print("=" * 60)

    try:
        # 运行基础使用示例
        await basic_usage_example()

    except Exception as e:
        print(f"💥 示例运行出错: {e}")
        raise


if __name__ == "__main__":
    # 运行所有示例
    asyncio.run(main())
