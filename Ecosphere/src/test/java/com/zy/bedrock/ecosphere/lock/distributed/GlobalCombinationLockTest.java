package com.zy.bedrock.ecosphere.lock.distributed;

import com.zy.bedrock.slate.lock.distributed.combination.DistributedGlobalCombinationLockFactory;
import com.zy.bedrock.slate.lock.distributed.combination.RedisDistributedGlobalCombinationLock;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.data.redis.DataRedisTest;
import org.springframework.util.Assert;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

/**
 * @author zy(Azurite - Y);
 * @date 2023/12/23;
 * @description {@linkplain RedisDistributedGlobalCombinationLock } 测试
 */
@Slf4j
@DataRedisTest(properties = {
        "logging.file.name=./src/test/resources/GlobalCombinationLockTest.log",
        "logging.level.com.zy.bedrock=info",
        "logging.level.com.zy.bedrock.slate.lock.distributed=debug"})
public class GlobalCombinationLockTest {
    private Lock redisDistributedGlobalCombinationLock;
    private StatefulRedisConnection<String, String> redisConnection;
    private final String DISTRIBUTE_LOCK_KEY = "redis";

    @BeforeEach
    public void before() {
        // azurite-redis-multiple-master01-svc
        RedisURI redisURI1 = RedisURI.Builder
                .redis("192.168.200.207")
                .withPort(30752)
                .withAuthentication("default", "123456")
                .build();
        //创建连接客户端
        RedisClient client = RedisClient.create(redisURI1);
        redisConnection = client.connect();

        // azurite-redis-multiple-master02-svc
        RedisURI redisURI2 = RedisURI.Builder
                .redis("192.168.200.207")
                .withPort(30209)
                .withAuthentication("default", "123456")
                .build();

        // azurite-redis-multiple-master03-svc
        RedisURI redisURI3 = RedisURI.Builder
                .redis("192.168.200.207")
                .withPort(30511)
                .withAuthentication("default", "123456")
                .build();

        RedisURI[] redisURIS = {redisURI1, redisURI2, redisURI3};
        DistributedGlobalCombinationLockFactory globalCombinationLockFactory =
                new DistributedGlobalCombinationLockFactory(redisURIS, DISTRIBUTE_LOCK_KEY, 6, 6, 4);
        this.redisDistributedGlobalCombinationLock = globalCombinationLockFactory.newGlobalRedisDistributedLockInstall();

        delDistributeLoc(redisURIS);
    }

    @AfterEach
    public void after() {
        redisConnection.close();
    }

    /**
     * 为保证测试的重复进行，而先清理分布式联合锁
     */
    private void delDistributeLoc(RedisURI[] redisURIArr) {
        for (RedisURI redisURI : redisURIArr) {
            RedisClient.create(redisURI).connect().sync().del(DISTRIBUTE_LOCK_KEY);
        }
    }

    /**
     * 单次加解锁耗时
     */
    @Test
    public void lockTime() {
        /*
         * logging.level = info
         *
         * 10ms ~ 12ms
         */
        long startTime = System.currentTimeMillis();

        try {
            // 加锁
            redisDistributedGlobalCombinationLock.lock();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            redisDistributedGlobalCombinationLock.unlock();
        }

        long endTime = System.currentTimeMillis();
        System.out.println("--costTime：" + (endTime - startTime) + " 毫秒--");
    }

	/**
	 * 锁延期测试
	 */
    @Test
    public void lockDelay() {
        try {
            // 加锁
            redisDistributedGlobalCombinationLock.lock();

            try {
                TimeUnit.SECONDS.sleep(20);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
			redisDistributedGlobalCombinationLock.unlock();
        }
    }


    /**
     * 多线程竞争测试
     */
    @Test
    public void MultiThreadedContention() {
        long startTime = System.currentTimeMillis();

        CountDownLatch countDownLatch = new CountDownLatch(4);

        new Thread(() -> {
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try {
                redisDistributedGlobalCombinationLock.lock();
                log.info("加锁成功 :: t1");
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                redisDistributedGlobalCombinationLock.unlock();
                countDownLatch.countDown();
            }
        }, "t1").start();

        new Thread(() -> {
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try {
                redisDistributedGlobalCombinationLock.lock();
                log.info("加锁成功 :: t2");
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                redisDistributedGlobalCombinationLock.unlock();
                countDownLatch.countDown();
            }
        }, "t2").start();

        new Thread(() -> {
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try {
                redisDistributedGlobalCombinationLock.lock();
                log.info("加锁成功 :: t3");
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                redisDistributedGlobalCombinationLock.unlock();
                countDownLatch.countDown();
            }
        }, "t3").start();

        new Thread(() -> {
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try {
                redisDistributedGlobalCombinationLock.lock();
                log.info("加锁成功 :: t4");
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                redisDistributedGlobalCombinationLock.unlock();
                countDownLatch.countDown();
            }
        }, "t4").start();

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        long endTime = System.currentTimeMillis();
        System.out.println("--costTime：" + (endTime - startTime) + " 毫秒--");
    }

    /**
     * 3000 人抢购一空,并公示其斩获
     */
    @Test
    public void flashSale() {
        /*
         * logging.level = info
         *
         * 1000 --> 5.2s
         * 10000 --> 48.7s ~ 49.1s
         */
        long startTime = System.currentTimeMillis();
        RedisCommands<String, String> redisCommands = redisConnection.sync();

        // 库存
        String inventoryKey = "inventory";
        int inventoryCount = 1000;
        // 设置库存
        redisCommands.set(inventoryKey, String.valueOf(inventoryCount));
        CountDownLatch countDownLatch = new CountDownLatch(inventoryCount);

        // 购买力
        String purchasingPower = "behead";
        int consumerCount = 3000;
        redisCommands.del(purchasingPower);

        ExecutorService es = Executors.newFixedThreadPool(consumerCount, new ThreadFactory() {
            final AtomicInteger consumer = new AtomicInteger();

            @Override
            public Thread newThread(@NonNull Runnable r) {
                Assert.notNull(r, "执行的 Runnable 不能为 null");
                return new Thread(r, String.valueOf(consumer.incrementAndGet()));
            }
        });

        for (int i = 1; i <= inventoryCount; i++) {
            es.submit(() -> {
                // 加锁
                try {
                    redisDistributedGlobalCombinationLock.lock();

                    String name = Thread.currentThread().getName();
                    log.info("消费者: {} :: 购买之后库存: '{}'", name, redisCommands.incrby(inventoryKey, -1));
                    // 购买之后递增斩获
                    redisCommands.hincrby(purchasingPower, name, 1);
                    countDownLatch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    // 解锁
                    redisDistributedGlobalCombinationLock.unlock();
                }
            });
        }

        try {
            log.info("{} :: 静候佳音...", Thread.currentThread().getName());
            countDownLatch.await();
            log.info("可喜可贺，库存售罄，财源广进...");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // ==== 统计 ====
        int minBehead = 0;
        int maxBehead = 0;
        double avgBehead = 0;

        List<String> entries = redisCommands.hvals(purchasingPower);
        int value;
        for (String val : entries) {
            value = Integer.parseInt(val);
            if (maxBehead == 0 || maxBehead < value) {
                maxBehead = value;
            } else if (minBehead == 0 || minBehead > value) {
                minBehead = value;
            }
            avgBehead += value;
        }

        log.info("销售详情 :: 单人最多购买量='{}' :: 单人最少购买量='{}' :: 单人平均购买量='{}'", maxBehead, minBehead, avgBehead / consumerCount);
        long endTime = System.currentTimeMillis();
        log.info("costTime：" + (endTime - startTime) + " 毫秒");
    }
}
