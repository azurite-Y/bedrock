package com.zy.bedrock.slate.lock.distributed.synchronizer;

import com.zy.bedrock.slate.lock.distributed.Competitor;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;


/**
 * @author zy(Azurite - Y);
 * @param <T> 竞争者类型
 * @date 2023/12/25;
 * @description 抽象的队列同步器，其维护一个有序访问流。这是一个双向的 FIFO 链表。
 * 依据这个有序访问流实现线程的等待与被动唤醒机制，从而避免了高并发下大量的 CAS 自旋等待竞争。
 */
@Slf4j
public abstract class AbstractQueuedSynchronizer<T  extends Competitor> extends AbstractExclusiveAccessSynchronizer<T> {
	protected volatile AtomicReference<CompetitorQueuedNode<T>> head;
	protected volatile AtomicReference<CompetitorQueuedNode<T>> tail;


	// ========================= 构造器 =========================
	/**
	 * 构建一个 AbstractQueuedSynchronizer 实例
	 */
	public AbstractQueuedSynchronizer() {
		this.head = new AtomicReference<>(null);
		this.tail = new AtomicReference<>(null);
	}


	// ========================= 公共方法 =========================
	// ============= 加锁 =============
	/**
	 * 阻塞等待的去竞争资源
	 *
	 * @param competitor 竞争者对象
	 */
	public final void competitor(T competitor) {
		if (tryCompetitor(competitor)) { // 第一次尝试
			competitor.afterCompetitor();

			if (log.isDebugEnabled()) {
				log.debug("首次尝试即竞争成功 :: Competitor={}, head={}", competitor, head.get() == null ? null : head.get().competitor);
			}
		} else if (doCompetitorQueued(appendCompetitorQueuedNode(competitor))) {
			selfInterrupt();
		}
	}

	/**
	 * 在指定时间段内尝试一次资源竞争
	 *
	 * @param nanosTimeout 超时纳秒时间参数
	 * @param competitor   竞争者对象
	 * @return 竞争成功则返回 true，反之则返回 false
	 */
	public final boolean tryCompetitorNanos(long nanosTimeout, T competitor) throws InterruptedException {
		if (Thread.interrupted()) {
			throw new InterruptedException();
		}

		if (tryCompetitor(competitor)) { // 第一次尝试
			competitor.afterCompetitor();

			if (log.isDebugEnabled()) {
				log.debug("首次尝试即竞争成功 :: Competitor={}, head={}", competitor, head.get() == null ? null : head.get().competitor);
			}
			return true;
		}
		return doCompetitorNanos(nanosTimeout, competitor);
	}

	/**
	 * 以可中断的方式竞争
	 *
	 * @param competitor 竞争者对象
	 * @throws InterruptedException 线程中断则抛出
	 */
	public final void competitorInterruptibility(T competitor) throws InterruptedException {
		if (Thread.interrupted()) {
			throw new InterruptedException();
		}
		if (tryCompetitor(competitor)) { // 第一次尝试
			competitor.afterCompetitor();
			if (log.isDebugEnabled()) {
				log.debug("首次尝试即竞争成功 :: Competitor={}, head={}", competitor, head.get() == null ? null : head.get().competitor);
			}
		} else {
			doCompetitorInterruptibility(competitor);
		}
	}

	// ============= 解锁 =============
	/**
	 * 释放竞争到的资源
	 *
	 * @param competitor 竞争者对象
	 */
	public final void release(T competitor) {
		if (tryRelease(competitor)) {
			competitor.afterRelease();

			if (head.get() != null && competitor == head.get().competitor) {
				unParkNextQueuedNode();
			} else {
				// 销毁当前竞争者对象
				competitor.destroy();
			}
		}
	}


	// ========================= 保护方法 =========================
	/**
	 * 判断当前是否需要阻塞当前线程，默认头节点直接重试竞争
	 *
	 * @param isHead 是否头节点
	 * @return true 则需要，反之则不需要
	 */
	protected boolean checkShouldPark(boolean isHead) {
		return !isHead;
	}
	/**
	 * 判断当前是否需要阻塞当前线程指定纳秒，默认头节点需阻塞
	 *
	 * @param isHead 是否头节点
	 * @return true 则需要，反之则不需要
	 */
	protected boolean checkShouldParkNanos(boolean isHead) {
		return isHead;
	}

	/**
	 * 追加竞争者到竞争者队列中
	 *
	 * @param competitor 竞争者对象
	 * @return 追加到队列中的的已创建竞争者节点
	 */
	protected CompetitorQueuedNode<T> appendCompetitorQueuedNode(T competitor) {
		CompetitorQueuedNode<T> createQueuedNode = new CompetitorQueuedNode<>(competitor, Thread.currentThread());

		if (tail.get() == null) { // 空队列则将当前节点设置为队列头节点和尾节点
			// CAS 更新头节点和尾节点
			if (tail.compareAndSet(null, createQueuedNode)) { // 竞争失败则追加到当前 tail 节点
				// head 在此第一次设值
				head.set(createQueuedNode);
				if (log.isDebugEnabled()) {
					log.debug("队列初始化 :: Competitor={}", competitor);
				}
				return createQueuedNode;
			}
		}
		CompetitorQueuedNode<T> prev;
		do {
            prev = tail.get();
			createQueuedNode.prev = prev;

			if (tail.compareAndSet(prev, createQueuedNode)) {
				// 此时 prev 已不再是 tail。已无竞争则可直接更新
				prev.next = createQueuedNode;
				break;
			}
        } while (true);

		if (log.isDebugEnabled()) {
			log.debug("追加队列节点 :: QueuedNode={}, prevQueuedNode={}", competitor, createQueuedNode.prev.competitor);
		}
		return createQueuedNode;
	}

	/**
	 * 尝试一次资源竞争
	 *
	 * @param competitor 竞争者对象
	 * @return 竞争成功则返回 true，反之则返回 false
	 */
	protected abstract boolean tryCompetitor(T competitor);

	/**
	 * 尝试释放竞争到的资源
	 *
	 * @param competitor 竞争者对象
	 * @return 释放成功则返回 true，反之则返回 false
	 */
	protected abstract boolean tryRelease(T competitor);


	// ========================= 私有方法 =========================
	/**
	 * 给定节点参与的队列竞争，竞争者线程将在队列中有序阻塞，直到其前序节点处理完成之后才会被唤醒。
	 *
	 * @param queuedNode 竞争者节点
	 * @return 竞争成功则返回 true，反之则返回 false
	 */
	private boolean doCompetitorQueued(CompetitorQueuedNode<T> queuedNode) {
		boolean failed = true;
		boolean isInterrupt = false;
		boolean isHead = false;
		try {
			if (log.isErrorEnabled()) {
				log.debug("开始队列竞争 :: QueuedNode={}, head={}", queuedNode.competitor, head.get() == null ? null : head.get().competitor);
			}
			for (; ; ) {
				if ((isHead = head.get() == queuedNode) && tryCompetitor(queuedNode.competitor)) {
					queuedNode.competitor.afterCompetitor();
					if (log.isDebugEnabled()) {
						log.debug("竞争成功 :: HeadQueuedNode={}", queuedNode.competitor);
					}

					failed = false;
					return isInterrupt;
				}

				if (checkShouldPark(isHead) && parkAndCheckInterrupt()) {
					isInterrupt = true;
				}
			}
		} finally {
			if (failed) { // 只有在 tryCompetitor 方法中才会抛出异常，转而打断循环。
				cancelCompetitor(isHead, queuedNode);
			}
		}

	}

	/**
	 * 在指定超时时间内给定节点参与的队列竞争，竞争者线程将在队列中有序阻塞，直到其前序节点处理完成之后才会被唤醒。
	 * 在被唤醒竞争成功之时，若超时则视为竞争失败。
	 *
	 * @param nanosTimeout 超时纳秒时间参数
	 * @param competitor   竞争者对象
	 * @return 竞争成功则返回 true，反之则返回 false
	 */
	private boolean doCompetitorNanos(long nanosTimeout, T competitor) throws InterruptedException {
		boolean failed = true;
		boolean isHead = false;
		long endTime = System.nanoTime() + nanosTimeout;
		CompetitorQueuedNode<T> queuedNode = appendCompetitorQueuedNode(competitor);
		try {
			if (log.isErrorEnabled()) {
				log.debug("开始队列竞争 :: QueuedNode={}, head={}", queuedNode.competitor, head.get() == null ? null : head.get().competitor);
			}
			for (; ; ) {
				if ((isHead = head.get() == queuedNode) && tryCompetitor(queuedNode.competitor)) {
					queuedNode.competitor.afterCompetitor();
					if (log.isDebugEnabled()) {
						log.debug("竞争成功 :: HeadQueuedNode={}", queuedNode.competitor);
					}

					failed = false;
					return true;
				}
				// 不管成功与否每次竞争之后都判断是否超时
				if (System.nanoTime() < endTime && checkShouldParkNanos(isHead)) {
					// 使用当前类作为同步器阻塞当前线程指定纳秒数
					LockSupport.parkNanos(this, 1000L);
				} else {
					return false;
				}
				// 检测中断标志位
				if (queuedNode.competitorThread.isInterrupted()) {
					throw new InterruptedException();
				}
			}
		} finally {
			if (failed) { // 只有在 tryCompetitor 方法中才会抛出异常，转而打断循环。
				cancelCompetitor(isHead, queuedNode);
			}
		}
	}

	/**
	 * 以可中断的方式竞争
	 *
	 * @param competitor 竞争者对象
	 * @throws InterruptedException 线程中断则抛出
	 */
	private void doCompetitorInterruptibility(T competitor) throws InterruptedException {
		boolean failed = true;
		boolean isHead = false;
		CompetitorQueuedNode<T> queuedNode = appendCompetitorQueuedNode(competitor);
		try {
			if (log.isDebugEnabled()) {
				log.debug("开始队列竞争 :: QueuedNode={}, head={}", queuedNode.competitor, head.get() == null ? null : head.get().competitor);
			}
			for (; ; ) {
				if ((isHead = head.get() == queuedNode) && tryCompetitor(queuedNode.competitor)) {
					queuedNode.competitor.afterCompetitor();
					if (log.isDebugEnabled()) {
						log.debug("竞争成功 :: HeadQueuedNode={}", queuedNode.competitor);
					}

					failed = false;
					break;
				}

				if (checkShouldPark(isHead) && parkAndCheckInterrupt()) {
					throw new InterruptedException();
				}
			}
		} finally {
			if (failed) { // 只有在 tryCompetitor 方法中才会抛出异常，转而打断循环。
				cancelCompetitor(isHead, queuedNode);
			}
		}
	}

	/**
	 * 阻塞当前线程，并在解除阻塞之后判断当前线程是否已被中断
	 *
	 * @return 若已被中断则返回 true
	 */
	private boolean parkAndCheckInterrupt() {
		if (log.isDebugEnabled()) {
			log.debug("阻塞线程，被动等待唤醒 :: {}", Thread.currentThread().getName());
		}

		LockSupport.park();
		return Thread.interrupted();
	}

	/**
	 * 取消竞争，可能在竞争中触发了异常
	 *
	 * @param isHead 竞争者节点是否头节点
	 * @param node   竞争者节点
	 */
	private void cancelCompetitor(boolean isHead, CompetitorQueuedNode<T> node) {
		Thread unparkThread = null;
		if (isHead) {
			headIteration();
			
			if (head.get() != null) {
				unparkThread = head.get().competitorThread;
			}
		} else { // 非头节点和尾节点只有取消操作才会去更新前后向索引，所以可直接修改
			unparkThread = node.next.competitorThread;

			// 前序节点的 next 指针指向后序节点
			node.prev.next = node.next;
			// 后序节点的 prev 指针指向前序节点
			node.next.prev = node.prev;
		}
		if (unparkThread != null) {
			if (log.isDebugEnabled()) {
				log.debug("竞争异常 :: 取消竞争者 {}, 唤醒线程 {}", node.competitor, unparkThread);
			}
			LockSupport.unpark(unparkThread);
		} else {
			if (log.isDebugEnabled()) {
				log.debug("竞争异常 :: 取消竞争者 {}, 无等待者", node.competitor);
			}
		}


		node.destroy();
	}

	/**
	 * 反转中断标志位
	 */
	private void selfInterrupt() {
		Thread.currentThread().interrupt();
	}

	/**
	 * 头节点迭代
	 */
	private void headIteration() {
		// 丢弃当前头节点并设置后序节点为头结点
		head.updateAndGet((first) -> {
			CompetitorQueuedNode<T> next = first.next;
			if (next != null) {
				// 方便 GC
				next.prev = null;
				first.next = null;
			} else {
				// 连带更新 tail(此时 head 和 tail 持有相同的对象)
				tail.compareAndSet(first, null);
			}
			return next;
		});
	}

	/**
	 * 解除阻塞的队列节点
	 */
	private void unParkNextQueuedNode() {
		CompetitorQueuedNode<T> competitorQueuedNode = head.get();

		// 节点更新
		headIteration();
		if (head.get() != null) {
			LockSupport.unpark(head.get().competitorThread);
		}

		if (log.isDebugEnabled()) {
			if (head.get() == null) {
				log.debug("队列中所有节点处理完成...");
			} else {
				log.debug("唤醒下一队列节点 :: CompetitorThread={}", head.get().competitorThread.getName());
			}
		}
		competitorQueuedNode.destroy();
	}

	
	// ========================= 内部类 =========================
	/**
	 * <p>竞争者队列节点，使用 {@linkplain AtomicReference } 保证属性节点添加和更新索引操作的原子性。</p>
	 *
	 * <p>具体运行结构如下。
	 * <span>队列初始状态:</span>
	 * <pre>
	 *       prev  +--------+     +--------+  prev
	 * null <----  |  head  |     |  tail  |  ----> null
	 * null <----  | (null) |     | (null) |  ----> null
	 *       next  +--------+     +--------+  next
	 *                ||              ||
	 *                \/              \/
	 *               (AR)            (AR)
	 * </pre>
	 *
	 * <span>空队列将追加 node1 节点为头结点和尾节点:</span>
	 * <pre>
	 *       prev  +-------+    prev   +-------+
	 * null <----  | head  |   <----   | tail  |
	 *             |(node1)|   ---->   |(node1)|  ----> null
	 *             +-------+   next    +-------+  next
	 *                 ||                 ||
	 *                 \/                 \/
	 *                (AR)               (AR)
	 * </pre>
	 *
	 * <span>队列已有节点将追加 node2 节点为末尾节点。</span>
	 * <pre>
	 *       prev  +-------+   prev   +-------+
	 * null <----  | head  |  <----   | tail  |
	 *             |(node1)|  ---->   |(node2)|  ----> null
	 *             +-------+  next    +-------+  next
	 *                ||                  ||
	 *                \/                  \/
	 *               (AR)                (AR)
	 * </pre>
	 * <span>以此类推...</span>
	 * <pre>
	 *       prev  +-------+   prev   +-------+   prev  +-------+
	 * null <----  | head  |  <----   |       |  <----  | tail  |
	 *             |(node1)|  ---->   |(node2)|  ---->  |(node3)|  ----> null
	 *             +-------+  next    +-------+  next   +-------+  next
	 *                ||                                    ||
	 *                \/                                    \/
	 *               (AR)                                  (AR)
	 * </pre>
	 * </p>
	 */
	private static final class CompetitorQueuedNode<T extends Competitor> {
		/** 前序节点 */
		private volatile CompetitorQueuedNode<T> prev;
		/** 后序节点 */
		private volatile CompetitorQueuedNode<T> next;
		/** 竞争者线程 */
		private Thread competitorThread;
		/** 锁竞争者 */
		private volatile T competitor;


		// ========================= 构造器 =========================
		/**
		 * 添加其他节点时使用的构造器
		 *
		 * @param competitor 竞争者对象
		 */
		public CompetitorQueuedNode(T competitor, Thread competitorThread) {
			this.competitor = competitor;
			this.competitorThread = competitorThread;
		}


		// ========================= 公共方法 =========================
		/** 有助于GC，销毁竞争者节点 */
		public void destroy() {
			// prev 和 next 指针已在队列操作中设置为 null
			this.competitor.destroy();
			this.competitor = null;
			this.competitorThread = null;
		}


		// ========================= equals、hashCode(仅比对 competitor 属性) =========================
		@Override
		public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
			CompetitorQueuedNode<?> that = (CompetitorQueuedNode<?>) o;
			return Objects.equals(competitor, that.competitor);
		}

		@Override
		public int hashCode() {
			return Objects.hash(competitor);
		}

		
        // ========================= toString(Debug 用途) =========================
		@Override
		public String toString() {
			return competitor.toString();
		}
	}
}
