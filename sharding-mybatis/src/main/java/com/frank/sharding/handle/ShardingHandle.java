package com.frank.sharding.handle;

import java.util.LinkedList;
import java.util.List;

import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;

import com.frank.sharding.annotation.ShardTable;
import com.frank.sharding.rule.DateRule;
import com.frank.sharding.rule.ParityRule;
import com.frank.sharding.rule.ShardRuleType;
import com.frank.sharding.rule.TotalRule;

public class ShardingHandle {
	
	MappedStatement statement;

	Executor executor;

	ShardTable table;

	Object parameter;

	RowBounds rowBounds;

	ResultHandler<Object> resultHandler;

	
	public ShardingHandle(MappedStatement statement, Executor executor, ShardTable table, Object parameter,
			RowBounds rowBounds, ResultHandler<Object> resultHandler) {
		this.statement = statement;
		this.executor = executor;
		this.table = table;
		this.parameter = parameter;
		this.rowBounds = rowBounds;
		this.resultHandler = resultHandler;
	}

	/**
	 * 按不同的分表规则处理
	 * @return
	 * @throws Exception
	 */
	public List<Object> sharding() throws Exception {
		List<Object> result = new LinkedList<>();
		switch (table.rule()) {
		case ShardRuleType.DATE:
			DateRule dateRuleHandle = new DateRule(statement, executor, table, parameter, rowBounds,
					resultHandler);
			result = dateRuleHandle.handle();
			break;
		case ShardRuleType.TOTAL:
			TotalRule totalRule = new TotalRule(statement, executor, table, parameter, rowBounds, resultHandler);
			result = totalRule.handle();
			break;
		case ShardRuleType.PARITY:
			ParityRule parityRule = new ParityRule(statement, executor, table, parameter, rowBounds, resultHandler);
			result = parityRule.handle();
			break;
		default:
			break;
		}
		return result;
	}

	public MappedStatement getStatement() {
		return statement;
	}

	public Executor getExecutor() {
		return executor;
	}

	public ShardTable getTable() {
		return table;
	}

	public Object getParameter() {
		return parameter;
	}

	public RowBounds getRowBounds() {
		return rowBounds;
	}

	public ResultHandler<Object> getResultHandler() {
		return resultHandler;
	}

}
