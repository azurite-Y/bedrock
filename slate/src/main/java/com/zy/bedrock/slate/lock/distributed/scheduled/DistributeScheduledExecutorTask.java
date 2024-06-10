package com.zy.bedrock.slate.lock.distributed.scheduled;


import com.zy.bedrock.slate.utils.Assert;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author zy(Azurite - Y);
 * @date 2023/12/4;
 * @description 使用定时任务线程池执行分布式周期任务
 */
public class DistributeScheduledExecutorTask extends AbstractDistributeScheduledTask {
    private ScheduledExecutorService postponeLockRate;

    public DistributeScheduledExecutorTask(long period) {
        super(period);
    }

    @Override
    public void startSchedule() {
        postponeLockRate = Executors.newScheduledThreadPool(1);
    }

    @Override
    public ScheduledFuture<?> addScheduledTask(Runnable runnable) {
        if (postponeLockRate == null) {
            throw new IllegalArgumentException("'postponeLockRate' 不能为 null");
        }
        // 等待间隔时间之后第一次任务，之后间隔固定时间执行任务
        return postponeLockRate.scheduleAtFixedRate(runnable, super.period, super.period, TimeUnit.SECONDS);
    }

    @Override
    public void stopSchedule(ScheduledFuture<?> scheduledFuture) {
        Assert.notNull(postponeLockRate, "'postponeLockRate' 不能为 null");
        Assert.notNull(scheduledFuture, "'scheduledFuture' 不能为 null");

        scheduledFuture.cancel(true);
    }

    @Override
    public void destroy() {
        postponeLockRate.shutdown();
        postponeLockRate = null;
    }
}
