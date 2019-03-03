package com.frank.sharding.rule;

import java.util.List;

import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMap;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;

import com.frank.sharding.annotation.ShardTable;

public abstract class ShardingRule {

	MappedStatement statement;

	Executor executor;

	ShardTable table;

	Object parameter;

	RowBounds rowBounds;

	ResultHandler<Object> resultHandler;


	public ShardingRule(MappedStatement statement, Executor executor, ShardTable table, Object parameter,
			RowBounds rowBounds, ResultHandler<Object> resultHandler) {
		this.statement = statement;
		this.executor = executor;
		this.table = table;
		this.parameter = parameter;
		this.rowBounds = rowBounds;
		this.resultHandler = resultHandler;
	}

	/**
	 * 子类需实现该方法
	 * @return
	 * @throws Exception
	 */
	public abstract List<Object> handle() throws Exception;

	/**构造新的MappedStatement
	 * @param ms
	 * @param newSqlSource
	 * @return
	 */
	public MappedStatement mappedStatement(MappedStatement ms, SqlSource newSqlSource) {
		MappedStatement.Builder builder = new MappedStatement.Builder(ms.getConfiguration(), ms.getId(), newSqlSource,
				ms.getSqlCommandType());
		builder.resource(ms.getResource());
		builder.fetchSize(ms.getFetchSize());
		builder.statementType(ms.getStatementType());
		builder.keyGenerator(ms.getKeyGenerator());
		if (ms.getKeyProperties() != null && ms.getKeyProperties().length != 0) {
			StringBuilder keyProperties = new StringBuilder();
			for (String keyProperty : ms.getKeyProperties()) {
				keyProperties.append(keyProperty).append(",");
			}
			keyProperties.delete(keyProperties.length() - 1, keyProperties.length());
			builder.keyProperty(keyProperties.toString());
		}
		builder.timeout(ms.getTimeout());
		ParameterMap parameterMap = ms.getParameterMap();
		builder.parameterMap(parameterMap);
		builder.resultMaps(ms.getResultMaps());
		builder.resultSetType(ms.getResultSetType());
		builder.cache(ms.getCache());
		builder.flushCacheRequired(ms.isFlushCacheRequired());
		builder.useCache(ms.isUseCache());
		return builder.build();
	}
}
