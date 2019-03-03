package com.frank.sharding.interceptor;

import java.util.Properties;

import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Plugin;
import org.apache.ibatis.plugin.Signature;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.springframework.core.annotation.Order;

import com.frank.sharding.annotation.ShardTable;
import com.frank.sharding.handle.ShardingHandle;

/**
 * @description mybatis 提交sql前拦截器，主要针对分表数据进行处理,采用的是多表查询合并
 * @author fengsheng
 * @since 2019年1月23日
 * @date 2019年1月23日
 */
@Intercepts({ @Signature(type = Executor.class, method = "query", args = { MappedStatement.class, Object.class,
		RowBounds.class, ResultHandler.class }) })
@Order(Integer.MAX_VALUE)
public class ShardInterceptor implements Interceptor {

	@Override
	@SuppressWarnings({ "unchecked" })
	public Object intercept(Invocation invocation) throws Throwable {
		Object[] objects = invocation.getArgs();
		MappedStatement statement = (MappedStatement) objects[0];
		String mapperPath = statement.getId();
		mapperPath = mapperPath.substring(0, mapperPath.lastIndexOf("."));
		ShardTable table = Class.forName(mapperPath).getAnnotation(ShardTable.class);
		if (table != null) {
			Executor executor = (Executor) invocation.getTarget();
			ShardingHandle shardingHandle = new ShardingHandle(statement,executor,table,objects[1],(RowBounds)objects[2],(ResultHandler<Object>)objects[3]);
			return shardingHandle.sharding();
		} else {
			return invocation.proceed();
		}
	}

	@Override
	public Object plugin(Object target) {
		return Plugin.wrap(target, this);
	}

	@Override
	public void setProperties(Properties properties) {
	}

}
