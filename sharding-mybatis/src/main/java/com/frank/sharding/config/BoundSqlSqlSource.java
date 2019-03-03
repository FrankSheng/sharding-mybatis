package com.frank.sharding.config;

import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.SqlSource;

/**
 * @description 包裝SqlSource
 * @author fengsheng
 * @since 2019年1月30日
 * @date 2019年1月30日
 */
public class BoundSqlSqlSource implements SqlSource {
    private BoundSql boundSql;

    public BoundSqlSqlSource(BoundSql boundSql) {
        this.boundSql = boundSql;
    }

    @Override
    public BoundSql getBoundSql(Object parameterObject) {
        return boundSql;
    }
}
