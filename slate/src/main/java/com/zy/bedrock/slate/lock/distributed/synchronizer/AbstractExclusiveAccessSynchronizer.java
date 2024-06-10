package com.zy.bedrock.slate.lock.distributed.synchronizer;

import com.zy.bedrock.slate.lock.distributed.Competitor;

/**
 * @author zy(Azurite - Y);
 * @date 2023/12/25;
 * @description 抽象的独占访问同步器，其维护的当前拥有独占访问权限的竞争者
 */
public abstract class AbstractExclusiveAccessSynchronizer<T extends Competitor> {
    protected volatile T competitor;


    public final T getCompetitor() {
        return competitor;
    }

    public final void setCompetitor(T competitor) {
        this.competitor = competitor;
    }
}
