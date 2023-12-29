package com.zy.bedrock.slate.lock.distributed;

/**
 * @author zy(Azurite - Y);
 * @date 2023/12/25;
 * @description 锁竞争者标记接口
 */
public interface Competitor {
    /**
     * 竞争资源成功之后调用的方法
     */
    default void afterCompetitor() {}

    /**
     * 竞争资源释放之后调用的方法
     */
    default void afterRelease() {}

    /**
     * 有助于GC，在竞争者退出竞争或销毁时调用的方法
     */
    default void destroy() {}
}
