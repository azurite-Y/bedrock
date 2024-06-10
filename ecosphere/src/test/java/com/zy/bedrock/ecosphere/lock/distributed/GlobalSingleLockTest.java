package com.zy.bedrock.ecosphere.lock.distributed;

import com.zy.bedrock.slate.lock.distributed.DistributedLockType;
import com.zy.bedrock.slate.lock.distributed.single.DistributedGlobalSingleLockFactory;
import com.zy.bedrock.slate.lock.distributed.single.RedisDistributedGlobalSingleLock;
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
 * @date 2023/12/24;
 * @description {@linkplain RedisDistributedGlobalSingleLock } 测试
 */
@Slf4j
@DataRedisTest(properties = {
		"logging.file.name=./src/test/resources/SingleLockTest.log",
		"logging.level.com.zy.bedrock=info",
		"logging.level.com.zy.bedrock.slate.lock.distributed=debug"})
public class GlobalSingleLockTest {
	private Lock redisDistributedGlobalSingleLock;
	private StatefulRedisConnection<String, String> redisConnection;
	private RedisCommands<String, String> redisCommands;
	private final String DISTRIBUTE_LOCK_KEY = "redis";

	/**
	 * 8ms 的锁竞争等待时间为单纯测试，实际应用中需根据 {@linkplain #lockTime() 单次加解锁耗时而} 而定
	 */
	@BeforeEach
	public void before() {
		RedisURI redisURI = RedisURI.Builder
				.redis("192.168.200.128")
				.withPort(30026)
				.withAuthentication("default","123456")
				.build();
		DistributedGlobalSingleLockFactory distributedGlobalSingleLockFactory =
				new DistributedGlobalSingleLockFactory(redisURI, DISTRIBUTE_LOCK_KEY, 6, 6);
		redisDistributedGlobalSingleLock = distributedGlobalSingleLockFactory.newGlobalSingleLockInstall(DistributedLockType.redis);

		// 创建连接客户端
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
		 * 连接 KubeSphere 单节点上部署 Redis 时：3ms ~ 5ms
		 * 连接 KubeSphere 三节点集群上部署 Redis 时：6ms ~ 7ms
		 */
		long startTime = System.currentTimeMillis();

		try {
			// 加锁
			redisDistributedGlobalSingleLock.lock();
		} catch (Exception e) {
			log.error("加锁异常", e);
		} finally {
			redisDistributedGlobalSingleLock.unlock();
		}
		long endTime = System.currentTimeMillis();
		log.info("costTime：" + (endTime - startTime) + " 毫秒");
	}

	@Test
	public void MultiThreadedContention() {
		long startTime = System.currentTimeMillis();
		CountDownLatch countDownLatch = new CountDownLatch(4);

		new Thread(()-> {
			try {
				TimeUnit.MILLISECONDS.sleep(100);
			} catch (InterruptedException e) {
				log.error("加锁异常", e);
			}
			try {
				redisDistributedGlobalSingleLock.lock();
				log.info("加锁成功 :: t1");
			} catch (Exception e) {
				log.error("加锁异常", e);
			} finally {
				redisDistributedGlobalSingleLock.unlock();
				countDownLatch.countDown();
			}
		}, "t1").start();

		new Thread(()-> {
			try {
				TimeUnit.MILLISECONDS.sleep(100);
			} catch (InterruptedException e) {
				log.error("加锁异常", e);
			}
			try {
				redisDistributedGlobalSingleLock.lock();
				log.info("加锁成功 :: t2");
			} catch (Exception e) {
				log.error("加锁异常", e);
			} finally {
				redisDistributedGlobalSingleLock.unlock();
				countDownLatch.countDown();
			}
		}, "t2").start();

		new Thread(()-> {
			try {
				TimeUnit.MILLISECONDS.sleep(100);
			} catch (InterruptedException e) {
				log.error("加锁异常", e);
			}
			try {
				redisDistributedGlobalSingleLock.lock();
				log.info("加锁成功 :: t3");
			} catch (Exception e) {
				log.error("加锁异常", e);
			} finally {
				redisDistributedGlobalSingleLock.unlock();
				countDownLatch.countDown();
			}
		}, "t3").start();

		new Thread(()-> {
			try {
				TimeUnit.MILLISECONDS.sleep(100);
			} catch (InterruptedException e) {
				log.error("加锁异常", e);
			}
			try {
				redisDistributedGlobalSingleLock.lock();
				log.info("加锁成功 :: t4");
			} catch (Exception e) {
				log.error("加锁异常", e);
			} finally {
				redisDistributedGlobalSingleLock.unlock();
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
		 * 连接 KubeSphere 单节点上部署 Redis 时：
		 * 		1000 --> 2.7s
		 * 		10000 --> 24.7s ~ 24.93s
		 * 连接 KubeSphere 三节点集群上部署 Redis 时：
		 * 		1000 --> 5.08s
		 * 		10000 --> 52.86s ~ 53.11s
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
					// 加锁
					redisDistributedGlobalSingleLock.lock();
					try {
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
						redisDistributedGlobalSingleLock.unlock();
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

	// ================== Main ==================
	public static void main(String[] args) {
		GlobalSingleLockTest test = new GlobalSingleLockTest();
		test.before();

		//		test.lockTime();
		test.MultiThreadedContention();

		test.after();
	}
}
