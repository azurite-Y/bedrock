package com.zy.bedrock.slate.lock.distributed.exception;

/**
 * @author zy(Azurite - Y);
 * @date 2023/12/3;
 * @description 不支持方法调用异常
 */
public class NotSupportedException extends RuntimeException {
    private static final long serialVersionUID = -2537376684783636653L;

    public NotSupportedException() {
        super();
    }

    public NotSupportedException(String s) {
        super(s);
    }
}
