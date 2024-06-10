package com.zy.bedrock.slate.lock.distributed;


import com.zy.bedrock.slate.lock.distributed.scheduled.DistributeScheduledExecutorTask;
import com.zy.bedrock.slate.lock.distributed.scheduled.DistributeScheduledTask;
import com.zy.bedrock.slate.utils.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zy(Azurite - Y);
 * @date 2023/12/21;
 * @description 分布式锁抽象类
 */
public abstract class AbstractDistributedLockFactory {
	protected Logger log = LoggerFactory.getLogger(getClass());
	
    /*
     * 分布式锁持有者名称前缀。可能用于标识各个微服务JVM
     */
    // protected final String ID_PREFIX = UUID.randomUUID().toString();
    /**
     * 分布式锁名称，适用与独锁和联合锁
     */
    protected final String DISTRIBUTE_LOCK_KEY;
    /**
     * 分布式锁失效时间,单位为秒
     * @apiNote 因为每次执行 Lua 脚本都需转换为 String, 所有直接定义为 String
     */
    protected String expireDate;
    /** 线程自旋等待时间，单位为毫秒 */
    protected int spinWaitTime;
    /** 分布式定时任务执行器。理论上定时任务会在每个分布式锁失效时间的一半时检查一次是否需要推迟锁的失效时间。*/
    protected DistributeScheduledTask distributeScheduledTask;


    // ========================= 构造器 =========================
    /**
     * AbstractDistributedLockFactory 构造方法
     *
     * @param distributedLockKey 分布式锁名称，适用与独锁和联合锁
     * @param expireDate 分布式锁失效时间,单位为秒
     * @param spinWaitTime 线程自旋等待时间，单位为毫秒
     */
    public AbstractDistributedLockFactory(String distributedLockKey, int expireDate, int spinWaitTime) {
        Assert.hasText(distributedLockKey, "'distributedLockKey' 不能为 null 或空串");
        Assert.isTrue(expireDate > 0, "'expireDate' 必须大于 0");
        Assert.isTrue(spinWaitTime > 0, "'spinWaitTime' 必须大于 0");
        this.DISTRIBUTE_LOCK_KEY = distributedLockKey;
        this.expireDate = String.valueOf(expireDate);
        this.spinWaitTime = spinWaitTime;

        distributeScheduledTask = new DistributeScheduledExecutorTask(expireDate/2);
        distributeScheduledTask.startSchedule();
        // this.initFactory();
    }

    // ========================= 公共方法 =========================
    /**
     * 初始化 Factory 实例方法。
     * @apiNote 无需在子类重写方法中调用，统一由归属类构造器调用以防止初始化遗漏步骤
     */
    public void initFactory() {}

    /**
     * 销毁 Factory 实例方法
     * @apiNote 必须在子类中调用
     */
    public void destroyFactory() {
        if (distributeScheduledTask != null) {
            distributeScheduledTask.destroy();
            distributeScheduledTask = null;
        }
    }
}
