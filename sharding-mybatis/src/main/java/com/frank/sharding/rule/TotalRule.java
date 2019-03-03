package com.frank.sharding.rule;

import java.util.List;

import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;

import com.frank.sharding.annotation.ShardTable;
/**
 * 按照表总数拆分
 * @author Administrator
 *
 */
public class TotalRule extends ShardingRule {

	public TotalRule(MappedStatement statement, Executor executor, ShardTable table, Object parameter,
			RowBounds rowBounds, ResultHandler<Object> resultHandler) {
		super(statement, executor, table, parameter, rowBounds, resultHandler);
	}

	/**
	 * 该方法暂未实现
	 */
	@Override
	public List<Object> handle() throws Exception {
		return null;
	}

	
}
