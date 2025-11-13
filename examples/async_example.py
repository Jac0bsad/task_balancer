import random
import time
from typing import Dict, Any
from task_balancer.manager import TaskQueueManager, TaskStatus
from task_balancer.utils.log_helper import logger


# 模拟任务函数
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


def demo_tasks():
    """
    演示异步任务管理器的完整使用流程
    """
    logger.info("🚀 开始异步任务管理器演示")

    # 1. 初始化管理器
    manager = TaskQueueManager(
        task_function=simulated_task,
        server_param_name="server_id",
        available_server_ids=["server_01", "server_02", "server_03", "server_04"],
        max_parallel_tasks=3,  # 最大并行任务数
        max_retries=2,  # 最大重试次数
    )

    try:
        # 2. 启动管理器
        manager.start()
        logger.info("✅ 任务管理器启动成功")

        # 3. 分批提交任务
        logger.info("📤 开始分批提交任务...")

        # 第一批任务
        batch1_tasks = [{"data": {"value": i, "batch": 1}} for i in range(5)]
        batch1_ids = manager.submit_tasks(batch1_tasks)
        logger.info("✅ 第一批提交 %d 个任务", len(batch1_ids))

        # 等待第一批任务部分完成
        time.sleep(2)

        # 第二批任务
        batch2_tasks = [{"data": {"value": i, "batch": 2}} for i in range(5, 10)]
        batch2_ids = manager.submit_tasks(batch2_tasks)
        logger.info("✅ 第二批提交 %d 个任务", len(batch2_ids))

        # 第三批任务（单个任务提交）
        single_task_id = manager.submit_single_task(
            {"data": {"value": 99, "batch": "single"}}
        )
        logger.info("✅ 单个任务提交: %s", single_task_id)

        # 4. 等待所有任务完成（最多等待30秒）
        logger.info("⏳ 等待所有任务完成...")
        all_completed = manager.wait_for_completion(timeout=30.0)

        if all_completed:
            logger.info("🎉 所有任务已完成!")
        else:
            logger.warning("⚠️  任务等待超时，部分任务可能仍在运行")

        # 5. 获取并显示任务结果
        logger.info("📊 任务结果统计:")

        successful_tasks = 0
        failed_tasks = 0

        for task_id in batch1_ids + batch2_ids + [single_task_id]:
            try:
                if manager.get_task_status(task_id) == TaskStatus.COMPLETED:
                    result = manager.get_task_result(task_id)
                    logger.info(
                        "✅ 任务 %s: 成功 - %s", task_id, result["processed_data"]
                    )
                    successful_tasks += 1
                else:
                    logger.info("❌ 任务 %s: 失败", task_id)
                    failed_tasks += 1
            except Exception as e:
                logger.error("⚠️  获取任务 %s 结果时出错: %s", task_id, e)
                failed_tasks += 1

        logger.info("📈 任务完成情况: %d 成功, %d 失败", successful_tasks, failed_tasks)

        # 6. 显示服务器统计信息
        server_status = manager.get_server_status()
        logger.info("🖥️  服务器统计:")
        for server_id, stats in server_status.items():
            logger.info(
                "   %s: %d 完成, %d 错误, %d 活跃",
                server_id,
                stats["total_completed"],
                stats["error_count"],
                stats["active_tasks"],
            )

    except Exception as e:
        logger.error("💥 演示过程中出错: %s", e)
    finally:
        # 7. 停止管理器
        manager.stop()
        logger.info("🛑 演示结束")


if __name__ == "__main__":
    demo_tasks()
