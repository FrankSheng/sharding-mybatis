package com.frank.sharding.rule;

import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.reflection.DefaultReflectorFactory;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.factory.DefaultObjectFactory;
import org.apache.ibatis.reflection.wrapper.DefaultObjectWrapperFactory;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;

import com.frank.sharding.annotation.ShardTable;
import com.frank.sharding.config.BoundSqlSqlSource;
import com.frank.sharding.handle.ResultHandle;
import com.frank.sharding.util.ObjectUtil;
import com.frank.sharding.util.TimeUtil;



public class DateRule extends ShardingRule {

	private static final String _1D = "_1d";
	private static final String _2D = "_2d";

	public DateRule(MappedStatement statement, Executor executor, ShardTable table, Object parameter,
			RowBounds rowBounds, ResultHandler<Object> resultHandler) {
		super(statement, executor, table, parameter, rowBounds, resultHandler);
	}

	@Override
	public List<Object> handle() throws Exception {
		// 获取传入对象的参数及值
		BoundSql sql = statement.getBoundSql(parameter);
		String reSql = sql.getSql();
		boolean ischange = false;
		if (!ObjectUtil.isBasicType(parameter) && !ObjectUtil.isString(parameter)) {
			if (parameter instanceof Date) {// 如果传入的查询条件是日期类型，将该时间作为一个时间点，判断该时间是否处于历史表或归档表
				int p = timePeriod((Date) parameter);
				if (p == 1) {
					reSql = reSql.replace(table.table(), table.table() + _1D);
				} else if (p == 2) {
					reSql = reSql.replace(table.table(), table.table() + _2D);
				}
				ischange = true;
			} else {
				Map<String, Object> imports = ObjectUtil.objectToMap(parameter);
				Set<Date> dates = new HashSet<>();
				imports.forEach((key, obj) -> {
					if (obj instanceof Date) {
						dates.add((Date) obj);
					} else if (obj instanceof String) {
						Date date = TimeUtil.isValidDate((String) obj);
						if (null != date) {
							dates.add(date);
						}
					}
				});
				List<Date> datesList = new LinkedList<>();
				datesList.addAll(dates);
				// 判断是一个时间参数还是两个时间参数，如果是一个时间参数，将该时间作为一个时间点，判断该时间是否处于历史表或归档表,
				// 如果是两个时间参数，那么判断两个时间段是处于当前表或历史表或者归档表，还是跨越了当前表和历史表或者历史表和归档表或者当前表到归档表。
				if (datesList.size() == 2) {
					int p1 = timePeriod(datesList.get(0));
					int p2 = timePeriod(datesList.get(1));
					ResultHandle handle = new ResultHandle();
					if ((p1 == 0 && p2 == 1) || (p1 == 1 && p2 == 0)) {
						return handle.shardQuery(statement, executor, parameter, rowBounds, resultHandler, table,
								table.table(), table.table() + _1D);
					} else if ((p1 == 0 && p2 == 2) || (p1 == 2 && p2 == 0)) {
						return handle.shardQuery(statement, executor, parameter, rowBounds, resultHandler, table,
								table.table(), table.table() + _1D, table.table() + _2D);
					} else if ((p1 == 1 && p2 == 2) || (p1 == 2 && p2 == 1)) {
						return handle.shardQuery(statement, executor, parameter, rowBounds, resultHandler, table,
								table.table() + _1D, table.table() + _2D);
					} else if (p1 == p2 && p1 == 1) {
						reSql = reSql.replace(table.table(), table.table() + _1D);
					} else if (p1 == p2 && p1 == 2) {
						reSql = reSql.replace(table.table(), table.table() + _2D);
					}
				} else if (dates.size() == 1) {
					int p = timePeriod(datesList.get(0));
					if (p == 1) {
						reSql = reSql.replace(table.table(), table.table() + _1D);
					} else if (p == 2) {
						reSql = reSql.replace(table.table(), table.table() + _2D);
					}
					ischange = true;
				}
			}
		}
		if (ischange) {
			MappedStatement newStatement = mappedStatement(statement, new BoundSqlSqlSource(sql));
			MetaObject msObject = MetaObject.forObject(newStatement, new DefaultObjectFactory(),
					new DefaultObjectWrapperFactory(), new DefaultReflectorFactory());
			msObject.setValue("sqlSource.boundSql.sql", reSql);
			statement = newStatement;
		}
		return executor.query(statement, parameter, rowBounds, resultHandler);
	}

	/**
	 * @Description: 获取传入的时间处于那个时间区间
	 * @since 2019年1月24日
	 * @date 2019年1月24日
	 * @param date
	 * @return 0 处于当前表 1 处于历史表 2 处于归档表
	 */
	private int timePeriod(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(new Date());
		calendar.add(Calendar.MONTH, -1);
		Date histDate = calendar.getTime();
		if (date.after(histDate)) {
			return 0;
		} else {
			calendar.add(Calendar.MONTH, -2);
			Date fileDate = calendar.getTime();
			if (date.after(fileDate)) {
				return 1;
			} else {
				return 2;
			}
		}
	}

}
