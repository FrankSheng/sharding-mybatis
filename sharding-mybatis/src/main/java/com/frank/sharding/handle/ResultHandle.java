package com.frank.sharding.handle;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMap;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.ResultMap;
import org.apache.ibatis.mapping.ResultMapping;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.reflection.DefaultReflectorFactory;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.factory.DefaultObjectFactory;
import org.apache.ibatis.reflection.wrapper.DefaultObjectWrapperFactory;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.springframework.util.StringUtils;

import com.frank.sharding.annotation.ShardTable;
import com.frank.sharding.config.BoundSqlSqlSource;
import com.frank.sharding.exception.SqlResolveException;
import com.frank.sharding.util.ObjectUtil;
import com.frank.sharding.util.PageableUtil;
import com.frank.sharding.util.ReflectUtil;
import com.frank.sharding.util.SqlResolve;

/**
 * @description 分表结果处理
 * @author fengsheng
 * @since 2019年1月30日
 * @date 2019年1月30日
 */
public class ResultHandle {

    private static final String SUM   = "sum";
    private static final String MIN   = "min";
    private static final String MAX   = "max";
    private static final String AVG   = "avg";
    private static final String COUNT = "count";

    /**
     * @Description: 对同一张表进行了水平拆分，数据存在多张表中，该方法会对传入的失SQL进行替换去所有分表中查询符合条件的数据
     * @since 2019年1月30日
     * @date 2019年1月30日
     * @param statement
     * @param executor
     * @param parameter
     * @param rowBounds
     * @param resultHandler
     * @param table
     *            原表
     * @param tableNames
     *            分表（不含原表）
     * @return
     * @throws Exception
     */
    public List<Object> shardQuery(MappedStatement statement, Executor executor, Object parameter, RowBounds rowBounds,
            ResultHandler<Object> resultHandler, ShardTable table, String... tableNames) throws Exception {
        List<ResultMap> resultMaps = statement.getResultMaps();
        ResultMap resultMap = resultMaps.get(0);
        List<ResultMapping> resultMappings = resultMap.getIdResultMappings();
        String sql = statement.getBoundSql(parameter).getSql();
        BoundSql boundSql = statement.getBoundSql(parameter);
        List<ParameterMapping> mappings = boundSql.getParameterMappings();
        Map<String, Object> paramMap = ObjectUtil.objectToMap(parameter);
        SqlResolve resolve = new SqlResolve(sql);
        Map<String, String> aggs = resolve.getAggregateFunction();
        List<String> groupBys = resolve.getGroupBy();
        Map<String, String> orderBys = resolve.getOrderBy();
        Map<String, Integer> limits = resolve.getLimit(mappings, paramMap);
        // 去除分页参数
        boolean isRemove = false;
        if (!limits.isEmpty()) {
            isRemove = true;
            int end = sql.indexOf("limit") < 0 ? sql.indexOf("LIMIT") : sql.indexOf("limit");
            sql = sql.substring(0, end);
        }
        /**
         * 1）判断是否有聚合函数； 1.1）聚合函数类型 1.2）是否有分组函数group by * having * 2）判断是否有分页参数；
         * 3)判断是否有排序参数；
         */
        List<Object> data = new LinkedList<>();
        for (int i = 0; i < tableNames.length; i++) {
            String mdSql = sql.replace(table.table(), tableNames[i]);
            MappedStatement newStatement = null;
            if (isRemove) {
                if (mappings.size() > 0) {
                    List<ParameterMapping> newmappings = mappings.subList(0, mappings.size() - 2);
                    BoundSql mdBoundSql = new BoundSql(statement.getConfiguration(), mdSql, newmappings, parameter);
                    newStatement = mappedStatement(statement, new BoundSqlSqlSource(mdBoundSql));
                }
            } else {
                newStatement = mappedStatement(statement, new BoundSqlSqlSource(statement.getBoundSql(parameter)));
                MetaObject msObject = MetaObject.forObject(newStatement, new DefaultObjectFactory(),
                        new DefaultObjectWrapperFactory(), new DefaultReflectorFactory());
                msObject.setValue("sqlSource.boundSql.sql", mdSql);
            }
            List<Object> querys = executor.query(newStatement, parameter, rowBounds, resultHandler);
            data.addAll(querys);
        }
        Map<String, List<Object>> groupDatas = new LinkedHashMap<>();
        for (Object value : data) {
            String key = "";
            for (String groupBy : groupBys) {
                String property = null;
                for (ResultMapping mapping : resultMappings) {
                    if (mapping.getColumn().toLowerCase().equals(groupBy)) {
                        property = mapping.getProperty();
                        break;
                    }
                }
                if (!StringUtils.isEmpty(property)) {
                    Object filedValue = ReflectUtil.getFieldValue(value, property);
                    if (null != filedValue) {
                        key += String.valueOf(filedValue);
                    }
                }
            }
            List<Object> list = groupDatas.get(key);
            if (null == list) {
                list = new LinkedList<>();
            }
            list.add(value);
            groupDatas.put(key, list);
        }
        List<Object> results = new ArrayList<Object>();
        // 处理聚合函数和分组，合并值情形
        if (!aggs.isEmpty()) {
            data.clear();
            groupDatas.forEach((gpro, datas) -> {
                if (resultMap.getType().getName().equals(Integer.class.getName())) {
                    aggs.forEach((column, fun) -> {
                        switch (fun) {
                            case COUNT:
                                int count = 0;
                                for (Object object : datas) {
                                    Integer re = (Integer) object;
                                    count += re;
                                }
                                data.add(count);
                                break;
                            case MIN:
                                int min = 0;
                                for (int i = 0; i < datas.size(); i++) {
                                    Integer re = (Integer) datas.get(i);
                                    if (i == 0) {
                                        min = re;
                                    } else if (min > re) {
                                        min = re;
                                    }
                                }
                                data.add(min);
                                break;
                            case SUM:
                                int sum = 0;
                                for (Object object : datas) {
                                    Integer re = (Integer) object;
                                    sum += re;
                                }
                                data.add(sum);
                                break;
                            case AVG:
                                throw new SqlResolveException("采用分表策略的sql不能使用数据库函数avg求平均值，只能查询出总和和总数，在代码里计算平均值。");
                            case MAX:
                                int max = 0;
                                for (int i = 0; i < datas.size(); i++) {
                                    Integer re = (Integer) datas.get(i);
                                    if (i == 0) {
                                        max = re;
                                    } else if (max < re) {
                                        max = re;
                                    }
                                }
                                data.add(max);
                                break;
                            default:
                                break;
                        }
                    });
                } else if (resultMap.getType().getName().equals(Long.class.getName())) {
                    aggs.forEach((column, fun) -> {
                        switch (fun) {
                            case COUNT:
                                long count = 0;
                                for (Object object : datas) {
                                    Long re = (Long) object;
                                    count += re;
                                }
                                data.add(count);
                                break;
                            case MIN:
                                long min = 0;
                                for (int i = 0; i < datas.size(); i++) {
                                    Long re = (Long) datas.get(i);
                                    if (i == 0) {
                                        min = re;
                                    } else if (min > re) {
                                        min = re;
                                    }
                                }
                                data.add(min);
                                break;
                            case SUM:
                                long sum = 0;
                                for (Object object : datas) {
                                    Long re = (Long) object;
                                    sum += re;
                                }
                                data.add(sum);
                                break;
                            case AVG:
                                throw new SqlResolveException("采用分表策略的sql不能使用数据库函数avg求平均值，只能查询出总和和总数，在代码里计算平均值。");
                            case MAX:
                                long max = 0;
                                for (int i = 0; i < datas.size(); i++) {
                                    Long re = (Long) datas.get(i);
                                    if (i == 0) {
                                        max = re;
                                    } else if (max < re) {
                                        max = re;
                                    }
                                }
                                data.add(max);
                                break;
                            default:
                                break;
                        }
                    });
                } else if (resultMap.getType().getName().equals(Double.class.getName())) {
                    aggs.forEach((column, fun) -> {
                        switch (fun) {
                            case COUNT:
                                double count = 0;
                                for (Object object : datas) {
                                    Double re = (Double) object;
                                    count += re;
                                }
                                data.add(count);
                                break;
                            case MIN:
                                double min = 0;
                                for (int i = 0; i < datas.size(); i++) {
                                    Double re = (Double) datas.get(i);
                                    if (i == 0) {
                                        min = re;
                                    } else if (min > re) {
                                        min = re;
                                    }
                                }
                                data.add(min);
                                break;
                            case SUM:
                                double sum = 0;
                                for (Object object : datas) {
                                    Double re = (Double) object;
                                    sum += re;
                                }
                                data.add(sum);
                                break;
                            case AVG:
                                throw new SqlResolveException("采用分表策略的sql不能使用数据库函数avg求平均值，只能查询出总和和总数，在代码里计算平均值。");
                            case MAX:
                                double max = 0;
                                for (int i = 0; i < datas.size(); i++) {
                                    Double re = (Double) datas.get(i);
                                    if (i == 0) {
                                        max = re;
                                    } else if (max < re) {
                                        max = re;
                                    }
                                }
                                data.add(max);
                                break;
                            default:
                                break;
                        }
                    });
                } else if (!resultMap.getType().getName().startsWith("java.lang")) {
                    Map<String, Object> maps = new LinkedHashMap<>();
                    aggs.forEach((column, fun) -> {
                        String property = null;
                        Class<?> propertyType = null;
                        for (ResultMapping mapping : resultMappings) {
                            if (mapping.getColumn().toLowerCase().equals(column)) {
                                property = mapping.getProperty();
                                propertyType = mapping.getJavaType();
                                break;
                            }
                        }
                        if (!StringUtils.isEmpty(property)) {
                            if (propertyType.getName().equals(Integer.class.getName())) {
                                Integer reInteger = 0;
                                for (int i = 0; i < datas.size(); i++) {
                                    Object object = datas.get(i);
                                    Integer pvalue = (Integer) ReflectUtil.getFieldValue(object, property);
                                    switch (fun) {
                                        case COUNT:
                                            reInteger += pvalue;
                                            break;
                                        case MIN:
                                            if (i == 0) {
                                                reInteger = pvalue;
                                            } else if (reInteger > pvalue) {
                                                reInteger = pvalue;
                                            }
                                            break;
                                        case SUM:
                                            reInteger += pvalue;
                                            break;
                                        case AVG:
                                            throw new SqlResolveException(
                                                    "采用分表策略的sql不能使用数据库函数avg求平均值，只能查询出总和和总数，在代码里计算平均值。");
                                        case MAX:
                                            if (i == 0) {
                                                reInteger = pvalue;
                                            } else if (reInteger < pvalue) {
                                                reInteger = pvalue;
                                            }
                                            break;
                                        default:
                                            break;
                                    }
                                }
                                maps.put(property, reInteger);
                            } else if (propertyType.getName().equals(Long.class.getName())) {
                                Long rLong = 0L;
                                for (int i = 0; i < datas.size(); i++) {
                                    Object object = datas.get(i);
                                    Long pvalue = (Long) ReflectUtil.getFieldValue(object, property);
                                    switch (fun) {
                                        case COUNT:
                                            rLong += pvalue;
                                            break;
                                        case MIN:
                                            if (i == 0) {
                                                rLong = pvalue;
                                            } else if (rLong > pvalue) {
                                                rLong = pvalue;
                                            }
                                            break;
                                        case SUM:
                                            rLong += pvalue;
                                            break;
                                        case AVG:
                                            throw new SqlResolveException(
                                                    "采用分表策略的sql不能使用数据库函数avg求平均值，只能查询出总和和总数，在代码里计算平均值。");
                                        case MAX:
                                            if (i == 0) {
                                                rLong = pvalue;
                                            } else if (rLong < pvalue) {
                                                rLong = pvalue;
                                            }
                                            break;
                                        default:
                                            break;
                                    }
                                }
                                maps.put(property, rLong);
                            } else if (propertyType.getName().equals(Double.class.getName())) {
                                Double rDouble = 0.0;
                                for (int i = 0; i < datas.size(); i++) {
                                    Object object = datas.get(i);
                                    Double pvalue = (Double) ReflectUtil.getFieldValue(object, property);
                                    switch (fun) {
                                        case COUNT:
                                            rDouble += pvalue;
                                            break;
                                        case MIN:
                                            if (i == 0) {
                                                rDouble = pvalue;
                                            } else if (rDouble > pvalue) {
                                                rDouble = pvalue;
                                            }
                                            break;
                                        case SUM:
                                            rDouble += pvalue;
                                            break;
                                        case AVG:
                                            throw new SqlResolveException(
                                                    "采用分表策略的sql不能使用数据库函数avg求平均值，只能查询出总和和总数，在代码里计算平均值。");
                                        case MAX:
                                            if (i == 0) {
                                                rDouble = pvalue;
                                            } else if (rDouble < pvalue) {
                                                rDouble = pvalue;
                                            }
                                            break;
                                        default:
                                            break;
                                    }
                                }
                                maps.put(property, rDouble);
                            }
                        }
                    });
                    Object reObject = datas.get(0);
                    maps.forEach((property, value) -> {
                        ReflectUtil.setFieldValue(reObject, property, String.valueOf(value));
                    });
                    data.add(reObject);
                }
            });
        }
        /**
         * 排序
         */
        if (!orderBys.isEmpty()) {
            for (String orderColumn : orderBys.keySet()) {
                ResultMapping mapping = null;
                for (ResultMapping rm : resultMappings) {
                    if (rm.getColumn().trim().equals(orderColumn)) {
                        mapping = rm;
                        break;
                    }
                }
                if (null != mapping) {
                    if (orderBys.get(orderColumn).equalsIgnoreCase("desc")) {
                        PageableUtil.sort(mapping.getProperty(), false, data);
                    } else {
                        PageableUtil.sort(mapping.getProperty(), true, data);
                    }
                }
            }
        }
        /**
         * 分页
         */
        if (!limits.isEmpty()) {
            results = PageableUtil.page(limits.get("start"), limits.get("start") + limits.get("end"), data);
        } else {
            results = data;
        }
        /**
         * 最终核验处理结果，防止查询报错
         */
        if (results.size() > 1) {
            String mappId = statement.getId();
            Class<?> mappClass = Class.forName(mappId.substring(0, mappId.lastIndexOf(".")));
            Method[] methods = mappClass.getMethods();
            for (Method method : methods) {
                if (method.getName().equals(mappId.substring(mappId.lastIndexOf(".") + 1))) {
                    Class<?> reClass = method.getReturnType();
                    String reType = reClass.getName();
                    if (!reType.substring(0, reType.lastIndexOf(".") < 0 ? 0 : reType.lastIndexOf(".")).equals(
                            "java.util")) {
                        results = results.subList(0, 1);
                        break;
                    }
                }
            }
        }
        return results;
    }

    /**
     * @Description: 构建 MappedStatement
     * @since 2019年2月19日
     * @date 2019年2月19日
     * @param ms
     * @param newSqlSource
     * @return
     */
    private MappedStatement mappedStatement(MappedStatement ms, SqlSource newSqlSource) {
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
