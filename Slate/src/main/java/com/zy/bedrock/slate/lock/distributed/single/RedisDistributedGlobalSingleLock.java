package com.zy.bedrock.slate.lock.distributed.single;

import com.zy.bedrock.slate.lock.distributed.AbstractGlobalDistributedLock;
import com.zy.bedrock.slate.lock.distributed.Competitor;
import com.zy.bedrock.slate.lock.distributed.scheduled.DistributeScheduledTask;
import com.zy.bedrock.slate.lock.distributed.synchronizer.AbstractQueuedSynchronizer;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;

import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author zy(Azurite - Y);
 * @date 2023/12/3;
 * @description Redis Hash 数据结构的分布式全局独锁。支持锁的互斥、重入性，能有效防止死锁的产生。
 */
public class RedisDistributedGlobalSingleLock extends AbstractGlobalDistributedLock {
    /** 同步器 */
    private Sync sync;
    /** 竞争者 ThreadLocal 实例 */
    private ThreadLocal<GlobalSingleLockCompetitor> competitorThreadLocal = new ThreadLocal<>();

    // ========================= Redis 访问类 =========================
    /* Redis 连接实例 */
    // private final StatefulRedisConnection<String, String> redisConnection;
    /** Redis 同步操作命令工具 */
    private RedisCommands<String, String> redisCommands;


    // ========================= 构造器 =========================
    /**
     * 构建一个 RedisDistributedGlobalSingleLock 实例
     *
     * @param redisCommands Redis 同步操作命令工具
     * @param distributeLockKey 分布式锁名称
     * @param expireDate 分布式锁失效时间,单位为秒
     * @param spinWaitTime 线程自旋等待时间，单位为毫秒
     * @param distributeScheduledTask 分布式定时任务执行器
     */
    public RedisDistributedGlobalSingleLock(RedisCommands<String, String> redisCommands, String distributeLockKey,
                                            String expireDate, int spinWaitTime, DistributeScheduledTask distributeScheduledTask) {
        super(distributeLockKey, expireDate, spinWaitTime, distributeScheduledTask);
        this.redisCommands = redisCommands;
        this.sync = new Sync();
    }


    // ========================= 公共方法 =========================
    @Override
    public void destroy() {
        this.sync = null;
        this.redisCommands = null;
        this.competitorThreadLocal = null;
        super.destroy();
    }

    // ========================= Lock 实现方法 =========================
    @Override
    public boolean tryLock() {
        return sync.tryCompetitor(getCompetitorNode());
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException{
        return sync.tryCompetitorNanos(unit.toNanos(time), getCompetitorNode());
    }
    @Override
    public void lockInterruptibly() throws InterruptedException {
        sync.competitorInterruptibility(getCompetitorNode());
    }

    @Override
    public void lock() {
        sync.competitor(getCompetitorNode());
    }

    @Override
    public void unlock() {
        sync.release(getCompetitorNode());

        // 释放锁之后清空 ThreadLocal
        competitorThreadLocal.remove();
    }


    // ========================= 保护方法 =========================
    /**
     * 获得竞争者节点
     */
    protected GlobalSingleLockCompetitor getCompetitorNode() {
        GlobalSingleLockCompetitor competitorNode;
        if (null == (competitorNode = this.competitorThreadLocal.get())) {
            competitorNode = new GlobalSingleLockCompetitor(createLockHolderName());
            this.competitorThreadLocal.set(competitorNode);
        }
        return competitorNode;
    }


    // ========================= 私有方法 =========================


    // ========================= 内部类 =========================
    /** 竞争者节点，用以封装竞争者特有的数据 */
    private final class GlobalSingleLockCompetitor implements Competitor {
        /** 完整的分布式锁持有者名称 */
        private String lockHolderName;
        /** 锁延期的定时任务的 ScheduledFuture 实例 */
        private ScheduledFuture<?> scheduledFuture;


        // ========================= 构造器 =========================
        /**
         * 构建一个 {@linkplain GlobalSingleLockCompetitor } 实例
         *
         * @param lockHolderName 完整的分布式锁持有者名称
         */
        public GlobalSingleLockCompetitor(String lockHolderName) {
            this.lockHolderName = lockHolderName;
        }


        // ========================= 实现 =========================
        @Override
        public void afterCompetitor() {
            // 加锁成功之后激活锁延期定时任务
            if (this.scheduledFuture == null) { // 第一次加锁
                this.scheduledFuture = distributeScheduledTask.addScheduledTask(() -> {
                    // 响应中断。线程被中断之后不做任何事情，等待定时任务被移除
                    if (!Thread.currentThread().isInterrupted() && ((boolean) executeLuaScript(redisCommands,
                            lockDelayScript, ScriptOutputType.BOOLEAN, scriptKeys, postponeTime))) {
                        if (log.isDebugEnabled()) {
                            log.debug("延期成功 :: scriptKeys='{}' :: args: {} :: lockHolderName: {}",
                                    scriptKeys, postponeTime, lockHolderName);
                        }
                    }
                });
            }
        }

        @Override
        public void afterRelease() {
            distributeScheduledTask.stopSchedule(this.scheduledFuture);
            if (log.isDebugEnabled()) {
                log.debug("锁释放 :: lockHolderName={}", lockHolderName);
            }
        }

        @Override
        public void destroy() {
            this.lockHolderName = null;
            this.scheduledFuture = null;
        }

        
        // ========================= equals、hashCode(仅比对 lockHolderName 属性) =========================
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            GlobalSingleLockCompetitor that = (GlobalSingleLockCompetitor) o;
            return Objects.equals(lockHolderName, that.lockHolderName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(lockHolderName);
        }

        
        // ========================= toString(Debug 用途) =========================
        @Override
        public String toString() {
        	return "{lockHolderName=" + lockHolderName + "}";
        }
        
    }
    
    private final class Sync extends AbstractQueuedSynchronizer<GlobalSingleLockCompetitor> {
        /**
         * 尝试一次资源竞争
         *
         * @param competitor 竞争者对象
         * @return 竞争成功则返回 true，反之则返回 false
         */
        @Override
        public boolean tryCompetitor(GlobalSingleLockCompetitor competitor) {
            return lockToSingleRedis(redisCommands, competitor.lockHolderName);
        }

        @Override
        protected boolean checkShouldPark(boolean isHead) {
            if (isHead) {
                try {
                	// CAS 等待，稍后继续竞争（可能与远端其他微服务实例竞争或与当前本地其他线程竞争）
                	TimeUnit.MILLISECONDS.sleep(spinWaitTime);
                	if (log.isDebugEnabled()) {
						log.debug("CAS 竞争等待 :: {}", Thread.currentThread().getName());
					}
                } catch (InterruptedException e) {
                	e.printStackTrace();
                }
                return false;
            }
            return true;
        }

        /**
         * 尝试释放竞争到的资源
         *
         * @param competitor 竞争者对象
         * @return 释放成功则返回 true，反之代表还有重入锁需释放则返回 false
         */
        @Override
        public boolean tryRelease(GlobalSingleLockCompetitor competitor) {
            return unLocToSingleRedis(redisCommands, competitor.lockHolderName);
        }
    }
}
