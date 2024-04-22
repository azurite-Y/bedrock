package com.zy.bedrock.ecosphere.lock.distributed;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.boot.test.autoconfigure.data.redis.DataRedisTest;
import org.springframework.util.Assert;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zy(Azurite - Y);
 * @date 2023/12/6;
 * @description: Redission 测试
 */
@Slf4j
@DataRedisTest(properties = {
		"logging.file.name=./src/test/resources/RedissionLockTest.log",
		"logging.level.com.zy.bedrock=info"})
public class RedissionTest {
	private final String DISTRIBUTE_LOCK_KEY = "redis";
	private RedissonClient singleRedissonClient;
	private StatefulRedisConnection<String, String> redisConnection;
	private RedisCommands<String, String> redisCommands;

	@BeforeEach
	public void before() {
		Config redissonConfig = new Config();
        redissonConfig.useSingleServer()
                .setAddress("redis://192.168.200.127:30026")
//				.setAddress("redis://192.168.200.128:30026")
				.setPassword("123456");
        singleRedissonClient = Redisson.create(redissonConfig);

		// 创建连接客户端
		RedisURI redisURI = RedisURI.Builder
				.redis("192.168.200.127")
//				.redis("192.168.200.128")
				.withPort(30026)
				.withAuthentication("default","123456")
				.build();
		RedisClient client = RedisClient.create(redisURI);
		redisConnection = client.connect();
		this.redisCommands = redisConnection.sync();
		delDistributeLoc();
	}

	@AfterEach
	public void after() {
		redisConnection.close();
	}

	/** 为保证测试的重复进行，而预先清理分布式独锁 */
	private void delDistributeLoc() {
		redisCommands.del(DISTRIBUTE_LOCK_KEY);
	}


	/**
	 * 单次加解锁耗时
	 */
	@Test
	public void lockTime() {
		/*
		 * logging.level = info
		 *
		 * 连接 KubeSphere 单节点上部署 Redis 时：16ms ~ 18ms
		 * 连接 KubeSphere 三节点集群上部署 Redis 时：17ms ~ 19ms
		 */
		long startTime = System.currentTimeMillis();

		RLock lock = null;
		try {
			lock = singleRedissonClient.getLock(DISTRIBUTE_LOCK_KEY);
			// 加锁
			lock.lock();
		} catch (Exception e) {
			log.error("加锁异常", e);
		} finally {
			if (lock != null) {
				lock.unlock();
			}
		}
		long endTime = System.currentTimeMillis();
		log.info("costTime：" + (endTime - startTime) + " 毫秒");
	}

	/**
	 * 3000 人抢购一空,并公示其斩获
	 */
	@Test
	public void flashSale() {
		/*
		 * logging.level = info
		 *
		 * 连接 KubeSphere 单节点上部署 Redis 时：
		 * 		1000 --> 3.209s ~ 3.309s
		 * 		10000 --> 31.337s ~ 32.755s
		 * 连接 KubeSphere 三节点集群上部署 Redis 时：
		 * 		1000 --> 7.306s ~ 7.406s
		 * 		10000 --> 78.636s ~ 78.902s
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

		try {
			for (int i = 1; i <= inventoryCount; i++) {
				es.submit(() -> {
					RLock lock = null;
					try {
						lock = singleRedissonClient.getLock(DISTRIBUTE_LOCK_KEY);
						// 加锁
						lock.lock();
						String name = Thread.currentThread().getName();
						log.info("消费者: {} :: 购买之后库存: '{}'", Thread.currentThread().getName(),
								redisCommands.incrby(inventoryKey, -1));
						// 购买之后递增斩获
						redisCommands.hincrby(purchasingPower, name, 1);
						countDownLatch.countDown();
					} catch (Exception e) {
						log.error("加锁异常", e);
					} finally {
						// 解锁
						if (lock != null) {
							lock.unlock();
						}
					}
				});
			}


			log.info("{} :: 静候佳音...", Thread.currentThread().getName());
			countDownLatch.await();
			log.info("可喜可贺，库存售罄，财源广进...");
		} catch (InterruptedException e) {
			es.shutdownNow();
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

		log.info("销售详情 :: 单人最多购买量='{}' :: 单人最少购买量='{}' :: 单人平均购买量='{}'", maxBehead, minBehead, avgBehead/consumerCount);
		long endTime = System.currentTimeMillis();
		log.info("costTime：" + (endTime - startTime) + " 毫秒");
	}
}
