package com.zy.bedrock.slate.lock.distributed.combination;


import com.zy.bedrock.slate.lock.distributed.AbstractDistributedLockFactory;
import com.zy.bedrock.slate.utils.Assert;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

/**
 * @author zy(Azurite - Y);
 * @date 2023/12/20;
 * @description 针对多 Redis 实例的分布式全局联合锁工厂类。下辖独锁采用 CAS 竞争锁
 */
public class DistributedGlobalCombinationLockFactory extends AbstractDistributedLockFactory {
    /**
     * Redis 连接 URI 数组
     */
    private RedisURI[] redisUriArr;
    /**
     * Redis 连接实例节点集，其排序即为加锁顺序。
     * 排序通过 ping-pong 机制得到的响应速度确定(在程序初始化时，响应速度较快的总是靠前)。
     */
    private RedisConnectionNode[] redisServerNodes;
    /**
     * 独锁操作时间,单位为毫秒
     * @apiNote 为了更加直观的把控联合锁中各个独锁的使用效率而定了。
     * 通过其可以显而易见的得到整个联合锁的加锁超时时间
     */
    private int singleLockOperationTime;
    /** 分布式全局联合锁 */
    private RedisDistributedGlobalCombinationLock redisDistributedGlobalCombinationLock;

    // ========================= 不可用节点自动重试 =========================
    /* 不可用节点间隔重试时间，单位为毫秒。默认为15分钟 */
    private long retryLockIntervalTime = 9000000;


    // ========================= 构造器 =========================
    /**
     * 构建一个 DistributedGlobalCombinationLockFactory 实例，
     * 设置分布式锁失效时间和线程自旋等待时间。同时使用默认的 5ms 的 Redis 命令执行超时时间
     *
     * @param redisUriArr Redis 连接 URI 数组
     * @param distributedLockKey 分布式锁名称，适用与独锁和联合锁
     * @param expireDate 分布式锁失效时间,单位为秒
     * @param spinWaitTime 线程自旋等待时间，单位为毫秒
     * @param singleLockOperationTime 独锁操作时间,单位为毫秒
     */
    public DistributedGlobalCombinationLockFactory(RedisURI[] redisUriArr, String distributedLockKey, int expireDate,
                                                   int spinWaitTime, int singleLockOperationTime) {
        super(distributedLockKey, expireDate, spinWaitTime);
        Assert.notNull(redisUriArr, "'redisUriArr' 不能为 null 或空数组");
        Assert.isTrue(singleLockOperationTime > 0, "'singleLockOperationTime' 必须大于 0");
        this.redisUriArr = redisUriArr;
        this.singleLockOperationTime = singleLockOperationTime;

        this.initFactory();
    }

    /**
     * 构建一个 DistributedGlobalCombinationLockFactory 实例，并设置相关时间参数
     *
     * @param redisUriArr Redis 连接 URI 数组
     * @param distributedLockKey 分布式锁名称，适用与独锁和联合锁
     * @param expireDate 分布式锁失效时间,单位为秒
     * @param spinWaitTime 线程自旋等待时间，单位为毫秒
     * @param singleLockOperationTime 独锁操作时间,单位为毫秒
     * @param retryLockIntervalTime 不可用节点重试加锁间隔时间，单位为分钟。默认为15分钟
     */
    public DistributedGlobalCombinationLockFactory(RedisURI[] redisUriArr, String distributedLockKey, int expireDate,
                                                   int spinWaitTime, int singleLockOperationTime, int retryLockIntervalTime) {
        this(redisUriArr, distributedLockKey, expireDate, spinWaitTime, singleLockOperationTime);
        Assert.isTrue(retryLockIntervalTime > 0, "'retryLockIntervalTime' 必须大于 0");
        this.retryLockIntervalTime = retryLockIntervalTime * 60000L;
    }


    // ========================= 公共方法 =========================
    public Lock newGlobalRedisDistributedLockInstall() {
        if (redisDistributedGlobalCombinationLock == null) {
            redisDistributedGlobalCombinationLock = new RedisDistributedGlobalCombinationLock(this.redisServerNodes, super.DISTRIBUTE_LOCK_KEY,
                    super.expireDate, super.spinWaitTime, singleLockOperationTime, this.retryLockIntervalTime, super.distributeScheduledTask);
        }
        return redisDistributedGlobalCombinationLock;
    }


    // ========================= 公共方法 =========================
    @Override
    public void initFactory() {
        if (singleLockOperationTime == 0) {
            singleLockOperationTime = 80;
        }

        redisServerNodes = new RedisConnectionNode[this.redisUriArr.length];
        createSingleLockServerCommands();
    }

    @Override
    public void destroyFactory() {
        this.redisDistributedGlobalCombinationLock.destroy();
        this.redisDistributedGlobalCombinationLock = null;
        for (RedisConnectionNode node : redisServerNodes) {
            node.redisCommands = null;
            node.redisConnection.closeAsync();
            node.redisConnection = null;
            node.host = null;
            node.available = null;
            node.unavailableNodeExceptionMessage = null;
            node.unavailableLockStartTime = null;
        }
        this.redisServerNodes = null;
        this.redisUriArr = null;
        super.destroyFactory();
    }


    // ========================= 私有方法 =========================
    /**
     * 创建连接对象同时进行锁清理
     */
    private void createSingleLockServerCommands() {
        RedisClient client;
        for (int i = 0; i < redisUriArr.length; i++) {
            // 验证
            validRedisUri(redisUriArr[i]);

            //创建连接客户端
            client = RedisClient.create(redisUriArr[i]);
//            client = RedisClient.create(ClientResources.builder().nettyCustomizer(createTimeoutReconnectionNettyCustomizer()).build(), redisUriArr[i]);
            // 启用连接丢失时的自动重新连接
            client.setOptions(ClientOptions.builder().autoReconnect(true).build());

            StatefulRedisConnection<String, String> connect = client.connect();
            // 设置一次只能发出一个目录
            connect.setAutoFlushCommands(true);

            redisServerNodes[i] = new RedisConnectionNode(redisUriArr[i].getHost() + ":" + redisUriArr[i].getPort(), connect);
        }
    }

    /**
     * 对 RedisURI 进行必要验证
     */
    private void validRedisUri(RedisURI redisUri) {
        Duration timeout = redisUri.getTimeout();
        if (timeout == null || timeout.get(ChronoUnit.SECONDS) <= 0) {
            if (log.isWarnEnabled()) {
                log.warn("建议设置大于 0 的 Redis 同步命令执行的命令超时时间, 否则将默认设置为 5ms");
            }
            redisUri.setTimeout(Duration.of(5, ChronoUnit.MILLIS));
        }
    }


    // ========================= 内部类 =========================
    /** Redis 连接节点(Master), 且限定当前类所在包内使用*/
    protected final class RedisConnectionNode {
        /** Redis 节点的标识，格式：ip:port */
        private String host;
        /**
         * Redis 连接实例，其排序即为加锁顺序。
         * 排序通过 ping-pong 机制得到的响应速度确定(在程序初始化时，响应速度较快的总是靠前)。
         */
        private StatefulRedisConnection<String, String> redisConnection;

        /** Redis 同步操作命令工具 */
        private RedisCommands<String, String> redisCommands;

        /** Redis 节点可用状态 */
        private AtomicBoolean available = new AtomicBoolean(true);

        /** Redis 节点不可用开始时间 */
        private AtomicLong unavailableLockStartTime = new AtomicLong(0);
        /** Redis 不可用节点触发的异常息 */
        private volatile String unavailableNodeExceptionMessage;

        /* 指标数据，Redis 节点不可用之后的上一次重试加锁时间 */
        // private AtomicLong unavailableLastRetryLockTime  = new AtomicLong(0);
        /* 指标数据，Redis 节点不可用之后的重试加锁次数 */
        // private AtomicLong unavailableRetryLockCount  = new AtomicLong(0);
        /* 指标数据，最后一次加锁成功时间 */
        // private AtomicLong lastLockedTime;

        /**
         * 构建一个代表 Redis 连接节点的 Node 实例
         * @param redisConnection - Redis 连接实例
         */
        public RedisConnectionNode(String host, StatefulRedisConnection<String, String> redisConnection) {
            this.host = host;
            this.redisConnection = redisConnection;
            this.redisCommands = redisConnection.sync();
        }

        // ========================= 公共方法 =========================
        public boolean setAvailable(boolean expect, boolean available) {
            return this.available.compareAndSet(expect, available);
        }

        // ========================= Getter、Setter =========================
        public String getHost() {
            return host;
        }
        public RedisCommands<String, String> getRedisCommands() {
            return redisCommands;
        }
        public boolean getAvailable() {
            return available.get();
        }

        public long getUnavailableLockStartTime() {
            return unavailableLockStartTime.get();
        }
        public void setUnavailableLockStartTime(long unavailableLockStartTime) {
            this.unavailableLockStartTime.set(unavailableLockStartTime);
        }
        public void setUnavailableNodeExceptionMessage(String unavailableNodeExceptionMessage) {
            this.unavailableNodeExceptionMessage = unavailableNodeExceptionMessage;
        }
        public String getUnavailableNodeExceptionMessage() {
            return unavailableNodeExceptionMessage;
        }
    }
}
