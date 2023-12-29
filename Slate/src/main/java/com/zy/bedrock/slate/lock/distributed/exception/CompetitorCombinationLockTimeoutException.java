package com.zy.bedrock.slate.lock.distributed.exception;

/**
 * @author zy(Azurite - Y);
 * @date 2023/12/29;
 * @description 联合锁竞争超时异常。若联合锁在限定时间内竞争锁超时则抛出此异常
 */
public class CompetitorCombinationLockTimeoutException extends RuntimeException {
	private static final long serialVersionUID = 3212109111091326649L;

	public CompetitorCombinationLockTimeoutException() {
        super();
    }

    public CompetitorCombinationLockTimeoutException(String s) {
        super(s);
    }
}
