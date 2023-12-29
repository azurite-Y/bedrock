package com.zy.bedrock.slate.lock.distributed.single;

import com.zy.bedrock.slate.lock.distributed.AbstractDistributedLockFactory;
import com.zy.bedrock.slate.lock.distributed.Assert;
import com.zy.bedrock.slate.lock.distributed.DistributedLockType;
import com.zy.bedrock.slate.lock.distributed.exception.NotSupportedException;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import java.util.concurrent.locks.Lock;

/**
 * @author zy(Azurite - Y);
 * @date 2023/12/13;
 * @description 针对单一分布式锁存储实例的工厂类，目的时创建一个全局使用的分布式独锁
 */
public class DistributedGlobalSingleLockFactory extends AbstractDistributedLockFactory {
    // ========================= Redis 连接属性 =========================
    /* Redis 连接 */
    // private RedisURI redisURI;
    /* Redis 连接客户端 */
    // private RedisClient redisClient;
    /** Redis 连接实例 */
    private StatefulRedisConnection<String, String> redisConnection;
    /** Redis 同步命令连接工具 */
    private RedisCommands<String, String> redisCommands;
    /** 全局的分布式独锁 */
    private RedisDistributedGlobalSingleLock redisDistributedGlobalSingleLock;

    // ========================= Zookeeper 连接属性 =========================


    // ========================= 构造器 =========================
    /**
     * 构建一个 DistributedSingleLockFactory 实例，分布式锁失效时间和线程自旋等待时间设置为默认值
     * @param redisURI - Redis 实例 URI
     */
    public DistributedGlobalSingleLockFactory(RedisURI redisURI, String distributedLockKey) {
        super(distributedLockKey, 6 , 50);
        createRedisConnection(redisURI);
    }

    /**
     * 构建一个 DistributedSingleLockFactory 实例，并设置分布式锁失效时间和线程自旋等待时间
     * @param redisURI - Redis 实例 URI
     * @param expireDate - 分布式锁失效时间,单位为秒
     * @param spinWaitTime - 线程自旋等待时间，单位为毫秒
     */
    public DistributedGlobalSingleLockFactory(RedisURI redisURI, String distributedLockKey, int expireDate, int spinWaitTime) {
        super(distributedLockKey, expireDate, spinWaitTime);
        createRedisConnection(redisURI);
    }


    // ========================= 公共方法 =========================
    /**
     * 根据给定分布式锁存储库中间件类型创建对应的分布式锁实例
     * <p>
     * 可用中间件如下:
     * <ul>
     *     <li>redis</li>
     *     <li>zookeeper</li>
     * </ul>
     * </p>
     * @param type - 中间件类型
     * @return 对应的分布式锁实例
     * @see DistributedLockType
     */
    public Lock newGlobalSingleLockInstall(DistributedLockType type) {
        switch (type) {
            case redis:
                return newRedisDistributedGlobalSingleLockInstall();
            case zookeeper:
                return newZookeeperDistributedGlobalSingleLockInstall();
            default:
                throw new IllegalArgumentException("不支持的类型: "+ type);
        }
    }

    @Override
    public void destroyFactory() {
        this.redisDistributedGlobalSingleLock.destroy();
        this.redisDistributedGlobalSingleLock = null;
        this.redisCommands = null;
        this.redisConnection.closeAsync();
        super.destroyFactory();
    }


    // ========================= 保护方法 =========================
    protected Lock newRedisDistributedGlobalSingleLockInstall() {
        if (this.redisCommands == null) {
            throw new NotSupportedException("不适用的 DistributedSingleLockFactory 实例构建，可能适用于 Zookeeper Lock");
        }
        Assert.notNull(super.distributeScheduledTask, "'distributeScheduledTask' 不能为 null");

        if (redisDistributedGlobalSingleLock == null) {
            redisDistributedGlobalSingleLock = new RedisDistributedGlobalSingleLock(this.redisCommands, super.DISTRIBUTE_LOCK_KEY, super.expireDate,
                super.spinWaitTime, super.distributeScheduledTask);
        }
        return redisDistributedGlobalSingleLock;
    }
    protected Lock newZookeeperDistributedGlobalSingleLockInstall() {
        return null;
    }


    // ========================= 私有方法 =========================
    private void createRedisConnection(RedisURI redisURI) {
        // redisURI 若未设置timeout则默认为60s
        Assert.notNull(redisURI, "'redisURI' 不能为 null");

        RedisClient redisClient = RedisClient.create(redisURI);
        this.redisConnection = redisClient.connect();
        this.redisCommands = this.redisConnection.sync();
    }
}
