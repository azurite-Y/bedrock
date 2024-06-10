package com.zy.bedrock.slate.lock.distributed.scheduled;


import com.zy.bedrock.slate.utils.Assert;

/**
 * @author zy(Azurite - Y);
 * @date 2023/12/4;
 * @description 分布式定时任务执行器
 */
public abstract class AbstractDistributeScheduledTask implements DistributeScheduledTask {
    /**
     * 定时任务执行间隔时间
     */
    protected long period;

    /**
     * 构建一个 AbstractDistributeScheduledTask 实例
     * @param period 定时任务执行间隔时间
     */
    public AbstractDistributeScheduledTask(long period) {
        Assert.isTrue(period > 0, "'corePoolSize' 不能小于0");
        this.period = period;
    }
}
