package com.frank.sharding.exception;

/**
 * @description 分表策略sql解析
 * @author fengsheng
 * @since 2019年2月22日
 * @date 2019年2月22日
 */
public class SqlResolveException extends RuntimeException {

    private static final long serialVersionUID = 6605093534005397838L;

    public SqlResolveException(String message) {
        super(message);
    }

    public SqlResolveException() {
    }
}
