package com.zy.bedrock.slate.lock.distributed.combination;

import com.zy.bedrock.slate.lock.distributed.AbstractGlobalDistributedLock;
import com.zy.bedrock.slate.utils.Assert;
import com.zy.bedrock.slate.lock.distributed.Competitor;
import com.zy.bedrock.slate.lock.distributed.exception.CompetitorCombinationLockTimeoutException;
import com.zy.bedrock.slate.lock.distributed.scheduled.DistributeScheduledTask;
import com.zy.bedrock.slate.lock.distributed.synchronizer.AbstractQueuedSynchronizer;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.ScriptOutputType;

import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author zy(Azurite - Y);
 * @date 2023/12/19;
 * @description 分布式全局联合锁
 */
public class RedisDistributedGlobalCombinationLock extends AbstractGlobalDistributedLock {
    /** 联合锁加锁超时等待时间 */
    private final int timeoutWaitTime;
    /*
     * 独锁操作时间
     * @apiNote 为了更加直观的控制联合锁中各个独锁的使用效率而定。
     * 通过其可以显而易见的得到整个联合锁的加锁超时时间
     */
    // private final int singleLockOperationTime;
    /** 不可用节点间隔重试时间，单位为毫秒。默认为15分钟 */
    private final long retryLockIntervalTime;

    /** 同步器 */
    private Sync sync;
    /** 竞争者 ThreadLocal 实例 */
    private ThreadLocal<GlobalCombinationLockCompetitor> competitorThreadLocal = new ThreadLocal<>();

    // ========================= Redis 访问类 =========================
    /** Redis 连接实例节点集 */
    private DistributedGlobalCombinationLockFactory.RedisConnectionNode[] redisServerNodes;


    // ========================= 构造器 =========================
    /**
     * 构建一个 RedisDistributedGlobalCombinationLock 实例
     *
     * @param redisServerNodes        Redis 连接节点集(Master)
     * @param distributeLockKey       分布式锁名称
     * @param expireDate              分布式锁失效时间,单位为秒
     * @param spinWaitTime            线程自旋等待时间，单位为毫秒
     * @param singleLockOperationTime 独锁操作时间
     * @param retryLockIntervalTime   不可用节点重试加锁间隔时间，单位为毫秒。默认为15分钟
     * @param distributeScheduledTask 分布式定时任务执行器
     */
    public RedisDistributedGlobalCombinationLock(DistributedGlobalCombinationLockFactory.RedisConnectionNode[] redisServerNodes,
                                                 String distributeLockKey, String expireDate, int spinWaitTime,
                                                 int singleLockOperationTime, long retryLockIntervalTime,
                                                 DistributeScheduledTask distributeScheduledTask) {
        super(distributeLockKey, expireDate, spinWaitTime, distributeScheduledTask);
        this.redisServerNodes = redisServerNodes;
        this.retryLockIntervalTime = retryLockIntervalTime;
        // this.singleLockOperationTime = singleLockOperationTime;
        this.timeoutWaitTime = singleLockOperationTime * redisServerNodes.length + 100;

        this.sync = new Sync();
    }


    // ========================= 公共方法 =========================
    @Override
    public void destroy() {
        this.sync = null;
        this.redisServerNodes = null;
        this.competitorThreadLocal = null;
        super.destroy();
    }


    // ========================= Lock 实现方法 =========================
    @Override
    public boolean tryLock() throws CompetitorCombinationLockTimeoutException {
        return sync.tryCompetitor(getCompetitorNode());
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException, CompetitorCombinationLockTimeoutException {
        return sync.tryCompetitorNanos(unit.toNanos(time), getCompetitorNode());
    }

    @Override
    public void lockInterruptibly() throws InterruptedException, CompetitorCombinationLockTimeoutException {
        sync.competitorInterruptibility(getCompetitorNode());
    }

    @Override
    public void lock() throws CompetitorCombinationLockTimeoutException { 
        sync.competitor(getCompetitorNode());
    }

    @Override
    public void unlock() {
    	GlobalCombinationLockCompetitor competitorNode = getCompetitorNode();
    	if (competitorNode.shouldUnlock) {
    		sync.release(competitorNode);
		}

        // 释放锁之后清空 ThreadLocal
        competitorThreadLocal.remove();
    }


    // ========================= 保护方法 =========================
    /** 获得竞争者节点 */
    protected GlobalCombinationLockCompetitor getCompetitorNode() {
        GlobalCombinationLockCompetitor competitorNode;
        if (null == (competitorNode = this.competitorThreadLocal.get())) {
            competitorNode = new GlobalCombinationLockCompetitor(createLockHolderName());
            this.competitorThreadLocal.set(competitorNode);
        }
        return competitorNode;
    }


    // ========================= 私有方法 =========================
    /**
     * 断言当前的 Redis 节点集是否满足最小可用节点集
     *
     * @param currentIndex 触发判断的 redis 节点下标
     * @param competitor   竞争者对象
     */
    private void assertMinAvailableRedisNodeNumber(int currentIndex, GlobalCombinationLockCompetitor competitor) {
        StringBuilder builder;
        // 最小可用节点数为 2
        if (2 > this.redisServerNodes.length - competitor.unAvailableNodeNumber) {
            unlock(currentIndex, competitor.lockHolderName);

            builder = new StringBuilder("加锁失败，当前 Redis 可用节点数小于联合锁最小可用节点数限制 :: lockHolderName");
            builder.append(competitor.lockHolderName).append(", ");
            for (DistributedGlobalCombinationLockFactory.RedisConnectionNode redisServerNode : this.redisServerNodes) {
                builder.append(redisServerNode.getHost())
                        .append(redisServerNode.getAvailable() ? "(可用)" : "(不可用)")
                        .append(" ");
            }
            throw new IllegalMonitorStateException(builder.toString());
        }
    }

    /**
     * 不可用节点间隔时间自动检测，判断给定节点当前是否任是不可用状态
     *
     * @param redisServerNode 给定测试节点
     * @return 仍是不可用状态或还未到达重试间隔则返回 true，若检测已可用则返回 false，
     */
    private boolean unAvailableNodeStatusIntervalTimeRetryCheck(DistributedGlobalCombinationLockFactory.RedisConnectionNode redisServerNode) {
        if (System.currentTimeMillis() - redisServerNode.getUnavailableLockStartTime() > retryLockIntervalTime) {
            try {
                redisServerNode.getRedisCommands().ping();

                redisServerNode.setAvailable(false, true);
                // Redis 节点不可用开始时间只有当前方法会被使用，所以等待该节点触发异常时刷新
                // singleLockServerNode.setUnavailableLockStartTime(0);
                return true;
            } catch (RedisConnectionException | RedisCommandTimeoutException redisException) {
                /* 忽略检查节点状态时触发的异常，但刷新不可用状态开始时间，做类似异常传播的效果 */
                redisServerNode.setUnavailableNodeExceptionMessage(redisException.getMessage());
                redisServerNode.setUnavailableLockStartTime(System.currentTimeMillis());
            }
        }
        return false;
    }

    /**
     * 解锁传入极限索引只能得独锁
     *
     * @param maxIndex 极限索引，需小于 {@linkplain #redisServerNodes } length
     * @return 全部解锁成功则返回true，反之则可能抛出异常
     * @apiNote 忽略解锁时触发的 {@linkplain RedisConnectionException} 和 {@linkplain RedisCommandTimeoutException } 异常。
     * 其他任何运行时异常均交由上层方法处理
     */
    private boolean unlock(int maxIndex, String lockHolderName) {
        Assert.isTrue(maxIndex < this.redisServerNodes.length, "'maxIndex' 值数组下标索引越界");
        
        DistributedGlobalCombinationLockFactory.RedisConnectionNode redisServerNode;
        for (int j = maxIndex; j > -1; j--) { // 逆序解锁，忽略触发的异常
        	redisServerNode = this.redisServerNodes[j];
        	try {
                if (redisServerNode.getAvailable()) {
                    unLocToSingleRedis(redisServerNode.getRedisCommands(), lockHolderName);
                    if (log.isDebugEnabled()) {
                        log.debug("解锁成功 :: lockHolderName={} :: i={}", lockHolderName, j);
                    }
                }
            } catch (RedisConnectionException | RedisCommandTimeoutException redisException) {
                // 在下层调用方法中此异常已记入日志，所以吞下该异常
                if (redisServerNode.getAvailable()) {
                    // 标记节点不可用
                    if (redisServerNode.setAvailable(true, false)) {
                    	redisServerNode.setUnavailableNodeExceptionMessage(redisException.getMessage());
                    	redisServerNode.setUnavailableLockStartTime(System.currentTimeMillis());
                    }
                    
                    if (log.isWarnEnabled()) {
                        log.warn("解锁中检测到 Redis 节点连接异常, 跳过该节点 :: lockHolderName={}, host={}, index={}, exceptionMsg={}",
                                lockHolderName, redisServerNode.getHost(), j, redisException.getMessage());
                    }
                }
			}
        }
        return true;
    }


    // ========================= 内部类 =========================
    /** 竞争者节点，用以封装竞争者特有的数据 */
    private final class GlobalCombinationLockCompetitor implements Competitor {
        /** 完整的分布式锁持有者名称 */
        private String lockHolderName;
        /** 锁延期的定时任务的 ScheduledFuture 实例数组 */
        private ScheduledFuture<?> scheduledFuture;
        /** 不可用节点数，不可用节点总是由竞争者节点自行发现 */
        private int unAvailableNodeNumber = 0;
        /** 是否需要解锁 */
        private boolean shouldUnlock = true;


        // ========================= 构造器 =========================
        /**
         * 构建一个 {@linkplain GlobalCombinationLockCompetitor } 实例
         *
         * @param lockHolderName 完整的分布式锁持有者名称
         */
        public GlobalCombinationLockCompetitor(String lockHolderName) {
            this.lockHolderName = lockHolderName;
        }


        // ========================= 实现 =========================
        @Override
        public void afterCompetitor() {
            // 加锁成功之后激活锁延期定时任务
            if (this.scheduledFuture == null) { // 第一次加锁
                this.scheduledFuture = distributeScheduledTask.addScheduledTask(() -> {
                    // 响应中断。线程被中断之后不做任何事情，等待定时任务被移除
                    if (!Thread.currentThread().isInterrupted()) {
                        for (DistributedGlobalCombinationLockFactory.RedisConnectionNode redisServerNode : redisServerNodes) {
                            if (redisServerNode.getAvailable()) {
                                executeLuaScript(redisServerNode.getRedisCommands(), lockDelayScript, ScriptOutputType.BOOLEAN, scriptKeys, postponeTime);
                                if (log.isDebugEnabled()) {
                                    log.debug("延期成功 :: scriptKeys='{}' :: args: {} :: lockHolderName: {}",
                                            scriptKeys, postponeTime, lockHolderName);
                                }
                            }

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
            GlobalCombinationLockCompetitor that = (GlobalCombinationLockCompetitor) o;
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

    private final class Sync extends AbstractQueuedSynchronizer<GlobalCombinationLockCompetitor> {
        /**
         * 尝试一次资源竞争
         *
         * @param competitor 竞争者对象
         * @return 竞争成功则返回 true，反之则返回 false
         */
        @Override
        protected boolean tryCompetitor(GlobalCombinationLockCompetitor competitor) throws CompetitorCombinationLockTimeoutException {
            // 顺序加锁，只有对首节点加锁成功才会尝试后续节点的加锁
            long startTime, endTime, costTime = 0;
            DistributedGlobalCombinationLockFactory.RedisConnectionNode redisServerNode;

            for (int i = 0; i < redisServerNodes.length; i++) {
                redisServerNode = redisServerNodes[i];

                if (!redisServerNode.getAvailable()) {
                    // 当前节点不可用，可能已被其他线程设置为不可用，立即检查是否满足最小可用节点集
                    competitor.unAvailableNodeNumber++;
                    assertMinAvailableRedisNodeNumber(i, competitor);

                    // 不可用节点状态间隔时间自动检测
                    if (unAvailableNodeStatusIntervalTimeRetryCheck(redisServerNode)) {
                        // 当前节点仍不可用且未突破底线则跳过当前节点，继续尝试对其他节点加锁
                        if (log.isWarnEnabled()) {
                            log.warn("加锁中检测到 Redis 节点连接异常, 跳过该节点 :: lockHolderName={}, host={}, index={}, exceptionMsg={}",
                                    competitor.lockHolderName, redisServerNode.getHost(), i, redisServerNode.getUnavailableNodeExceptionMessage());
                        }
                        continue;
                    }
                }

                // 当前节点可用(包含自动重试验证为可用的节点)
                try {
                    startTime = System.currentTimeMillis();

                    if (!lockToSingleRedis(redisServerNode.getRedisCommands(), competitor.lockHolderName)) {
                        // 未抛出异常则代表命令执行成功，但竞争失败，根据最左权重原则判断此次竞争失败
                        if (log.isDebugEnabled()) {
                            log.debug("据最左权重原则判定当前竞争者节点竞锁失败 :: lockHolderName={}, index={}", competitor.lockHolderName, i);
                        }
                        return false;
                    }

                    endTime = System.currentTimeMillis();
                    costTime += endTime - startTime;

                    if (costTime > timeoutWaitTime) { // 加锁超时
                        if (log.isDebugEnabled()) {
                            log.debug("联合锁超时加锁，释放已持有锁 :: lockHolderName={}, index={}", competitor.lockHolderName, i);
                        }
                        if (0 == i) { // 头节点
                            unLocToSingleRedis(redisServerNode.getRedisCommands(), competitor.lockHolderName);
                        } else { // 非头节点
                            unlock(i, competitor.lockHolderName);
                        }
                        /*
                         * 若从头节点开始竞争可能出现一个竞争者节点大量重试导致后序节点无法竞争的情况，
                         * 所以在此解锁后直接抛出异常。在保证同步队列吞吐量的同时也给上层方法知晓的机会。
                            i = -1;
                            costTime = 0;
                            if (log.isDebugEnabled()) {
                                log.debug("从头节点开始竞争 :: lockHolderName={}", competitor.lockHolderName);
                            }
                         */
                        // 已经解锁，之后将跳过 finally 块中的解锁操作
                        competitor.shouldUnlock = false;
                        throw new CompetitorCombinationLockTimeoutException("联合锁竞锁超时 :: timeoutWaitTime="
                                + timeoutWaitTime + ", costTime=" + costTime);
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("竞锁成功 :: lockHolderName={}, host={}, index={}, costTime={}",
                                    competitor.lockHolderName, redisServerNode.getHost(), i, costTime);
                        }
                    }
                } catch (RedisConnectionException | RedisCommandTimeoutException redisException) {
                    // 在下层调用方法中此异常已记入日志，所以吞下该异常
                    if (redisServerNode.getAvailable()) {
                        // 标记节点不可用
                        if (redisServerNode.setAvailable(true, false)) {
                            redisServerNode.setUnavailableNodeExceptionMessage(redisException.getMessage());
                            redisServerNode.setUnavailableLockStartTime(System.currentTimeMillis());
                        }
                    }

                    if (log.isWarnEnabled()) {
                        log.warn("加锁中检测到 Redis 节点连接异常, 跳过该节点 :: lockHolderName={}, host={}, index={}, exceptionMsg={}",
                                competitor.lockHolderName, redisServerNode.getHost(), i, redisException.getMessage());
                    }

                    // 立即检查当前的 Redis 节点集是否满足最小可用节点集
                    competitor.unAvailableNodeNumber++;
                    assertMinAvailableRedisNodeNumber(i, competitor);
                }
            }
            /*
             * 在最左权重原则下，在最左端竞争得锁之后，其后将无线程与之争抢。
             * 除非其加锁成功的 Redis 实例全部宕机，那么此时才需与其他线程进行竞争下一 Redis 实例的加锁。
             * 而一路加锁成功且至此始终满足最小可用节点数的限制则代表加锁成功。
             */
            if (log.isDebugEnabled()) {
            	log.debug("联合锁加锁成功 :: lockHolderName={}", competitor.lockHolderName);
            }
            return true;
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
         * @return 释放成功则返回 true，反之则返回 false
         */
        @Override
        protected boolean tryRelease(GlobalCombinationLockCompetitor competitor) {
            return unlock(redisServerNodes.length - 1, competitor.lockHolderName);
        }
    }
}
