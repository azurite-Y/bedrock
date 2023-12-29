package com.zy.bedrock.slate.lock.distributed;

import com.zy.bedrock.slate.lock.distributed.scheduled.DistributeScheduledTask;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * @author zy(Azurite - Y);
 * @date 2023/12/25;
 * @description 全局锁抽象类
 */
public abstract class AbstractGlobalDistributedLock implements Lock {
    protected Logger log = LoggerFactory.getLogger(getClass());

    /* 分布式锁持有者名称前缀 */
    // private final String ID_PREFIX = UUID.randomUUID().toString();
    /**
     * 分布式锁失效时间,单位为秒
     * @apiNote 因为每次执行 Lua 脚本都需转换为 String, 所有直接定义为 String
     */
    protected final String expireDate;
    /** 线程自旋等待时间，单位为毫秒 */
    protected final int spinWaitTime;
    /** 脚本锁使用的 keys 数组 */
    protected final String[] scriptKeys;

    // ========================= 加锁 =========================
    /**
     * 加锁 Lua 脚本</br>
     * 返回值: 0-加锁成功或锁重入，1-加锁失败
     *
     * <p>
     * <span>脚本:</span>
     * <pre> {@code
     *      if (redis.call('exists', KEYS[1])== 1)
     *      then
     *          if (redis.call('hexists', KEYS[1], ARGV[1]) == 1) // 锁重入
     *          then
     *              redis.call('hincrby', KEYS[1], ARGV[1], 1)
     *              redis.call('expire', KEYS[1], ARGV[2])
     *              return 1
     *          else // 已有其他线程持有分布式锁
     *              return 0
     *          end
     *      else // 无分布式锁存在
     *          redis.call('hincrby', KEYS[1], ARGV[1], 1)
     *          redis.call('expire', KEYS[1], ARGV[2])
     *          return 1
     *      end
     * }</pre>
     * <span>脚本入参:</span>
     * <ul>
     *     <li>KEYS[1]: distributeLockKey</li>
     *     <li>ARGV[1]: lockHolderName</li>
     *     <li>ARGV[2]: expireDate</li>
     * </ul></p>
     */
    protected final String lockScript = "if (redis.call('exists', KEYS[1])== 1) then if (redis.call('hexists', KEYS[1], ARGV[1]) == 1) then redis.call('hincrby', KEYS[1], ARGV[1], 1) redis.call('expire', KEYS[1], ARGV[2]) return 1 else return 0 end else redis.call('hincrby', KEYS[1], ARGV[1], 1) redis.call('expire', KEYS[1], ARGV[2]) return 1 end";

    // ========================= 解锁 =========================
    /**
     * 解锁 Lua 脚本</br>
     * 返回值：0-释放锁, 1-解锁但还有重入锁未释放, 2-解除非当前线程持有的锁
     *
     * <p>脚本:
     * <pre> {@code
     *      if (redis.call('hexists', KEYS[1], ARGV[1]))
     *          then
     *          if (redis.call('hincrby', KEYS[1], ARGV[1], -1) == 0) // 解锁
     *          then
     *              redis.call('del', KEYS[1]) // 解锁完毕删除锁对象
     *              return 0
     *          else
     *              redis.call('expire', KEYS[1], ARGV[2]) // 更新锁过期时间
     *              return 1
     *          end
     *      else
     *          return 2
     *      end
     * }</pre>
     *
     * <span>脚本入参:</span>
     * <ul>
     *     <li>KEYS[1]: distributeLockKey</li>
     *     <li>ARGV[1]: lockHolderName</li>
     *     <li>ARGV[2]: expireDate</li>
     * </ul></p>
     */
    protected final String unLockScript = "if (redis.call('hexists', KEYS[1], ARGV[1])) then if (redis.call('hincrby', KEYS[1], ARGV[1], -1) == 0) then redis.call('del', KEYS[1]) return 0 else redis.call('expire', KEYS[1], ARGV[2]) return 1 end else return 2 end";

    // ========================= 分布式锁续期 =========================
    /** 分布式定时任务执行器。理论上定时任务会在每个分布式锁失效时间的一半时检查一次是否需要推迟锁的失效时间。*/
    protected DistributeScheduledTask distributeScheduledTask;
    /**
     * 锁延期 Lua 脚本</br>
     * 返回值: 0-key不存在，1-延期成功
     *
     * <p>脚本:
     * <pre> {@code
     *      if (redis.call('exists', KEYS[1]))
     *      then
     *          redis.call('expire', KEYS[1], ARGV[1])
     *          return 1
     *      else
     *          return 0
     *      end
     * }</pre>
     *
     * <span>脚本入参:</span>
     * <ul>
     *     <li>KEYS[1]: distributeLockKey</li>
     *     <li>ARGV[1]: expireDate</li>
     * </ul></p>
     */
    protected final String lockDelayScript = "if (redis.call('exists', KEYS[1]) == 1) then redis.call('expire', KEYS[1], ARGV[1]) return 1 else return 0 end";
    /**
     * 锁过期的推迟时间
     * @apiNote 因为每次执行 Lua 脚本都需转换为 String, 所有直接定义为 String
     */
    protected final String postponeTime;


    // ========================= 构造器 =========================
    /**
     * 构建一个 RedisDistributedGlobalSingleLock 实例
     *
     * @param distributeLockKey 分布式锁名称
     * @param expireDate 分布式锁失效时间,单位为秒
     * @param spinWaitTime 线程自旋等待时间，单位为毫秒
     * @param distributeScheduledTask 分布式定时任务执行器
     */
    public AbstractGlobalDistributedLock(String distributeLockKey, String expireDate, int spinWaitTime,
                                         DistributeScheduledTask distributeScheduledTask) {
        // 参数已在 DistributedGlobalSingleLockFactory 效验
        this.expireDate = expireDate;
        this.spinWaitTime = spinWaitTime;
        this.distributeScheduledTask = distributeScheduledTask;

        this.scriptKeys= new String[]{ distributeLockKey };
        this.postponeTime = this.expireDate;
    }


    // ========================= 公共方法 =========================
    /**
     *  实例销毁方法
     * @apiNote 必须在子类中调用
     */
    public void destroy() {}

    // ========================= Lock 实现方法 =========================
    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }


    // ========================= 保护方法 =========================
    /** 创建完整的分布式锁持有者名称 */
    protected String createLockHolderName() {
        return UUID.randomUUID().toString() + ':' + Thread.currentThread().getName();
    }

    /**
     * 对给定的单一 Redis 实例加锁
     *
     * @param redisCommands Redis 同步操作命令工具
     * @param lockHolderName 完整的分布式锁持有者名称
     * @return 加锁成功则返回 true，反之则返回 false
     */
    protected boolean lockToSingleRedis(RedisCommands<String, String> redisCommands, String lockHolderName) {
        return (boolean) executeLuaScript(redisCommands, this.lockScript,
                ScriptOutputType.BOOLEAN, this.scriptKeys, lockHolderName, this.expireDate);
    }

    /**
     * 对给定的单一 Redis 实例解锁
     *
     * @param redisCommands Redis 同步操作命令工具
     * @param lockHolderName 完整的分布式锁持有者名称
     * @return 加锁成功则返回 true，反之则返回 false
     * @throws UnsupportedOperationException 解锁非自身持有的分布式锁时抛出
     */
    protected boolean unLocToSingleRedis(RedisCommands<String, String> redisCommands, String lockHolderName) {
        // 0-释放锁, 1-解锁但还有重入锁未释放, 2-解除非当前线程持有的锁
        long scriptReturnVal = (long) executeLuaScript(redisCommands, this.unLockScript, ScriptOutputType.INTEGER,
                this.scriptKeys, lockHolderName, this.expireDate);
        if (scriptReturnVal == 0) {
            return true;
        } else if (scriptReturnVal == 2) {
            throw new UnsupportedOperationException("不支持解锁非自身持有的分布式锁 :: " + lockHolderName);
        }
        return false;
    }

    /**
     * Lua 脚本执行
     *
     * @param redisCommands Redis 同步操作命令工具
     * @param evalScript Lua 脚本
     * @param scriptOutputType Lua 脚本返回值类型
     * @param keys Lua 脚本使用的 key 数组
     * @param args Lua 脚本使用的参数
     * @return Lua 脚本返回值
     */
    protected Object executeLuaScript(RedisCommands<String, String> redisCommands, String evalScript, ScriptOutputType scriptOutputType, String[] keys, String... args) {
        try {
            return redisCommands.eval(evalScript, scriptOutputType, keys, args);
        } catch (RedisCommandTimeoutException commandTimeoutException) {
            log.error("Redis 命令执行超时异常 :: " + evalScript, commandTimeoutException);
            throw commandTimeoutException;
        } catch (RedisConnectionException connectionException) {
            log.error("Redis 连接异常异常", connectionException);
            throw connectionException;
        } catch (Exception e) {
            log.error("执行 Lua 脚本异常", e);
            throw e;
        }
    }
}
