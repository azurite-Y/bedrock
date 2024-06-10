package com.zy.bedrock.slate.lock.distributed.scheduled;

import java.util.concurrent.ScheduledFuture;

/**
 * @author zy(Azurite - Y);
 * @date 2023/12/4;
 * @description 分布式定时任务接口
 *
 */
public interface DistributeScheduledTask {
    /**
     * 创建定时任务线程池
     */
    void startSchedule();

    /**
     * 添加定时任务
     */
    ScheduledFuture<?> addScheduledTask(Runnable runnable);

    /**
     * 停止指定定时任务
     */
    void stopSchedule(ScheduledFuture<?> scheduledFuture);

    /**
     * 关闭定时任务线程池
     */
    void destroy();
}
