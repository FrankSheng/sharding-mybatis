package com.frank.sharding.util;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.ibatis.mapping.ParameterMapping;
import org.springframework.util.StringUtils;

/**
 * @description sql解析工具
 * @author fengsheng
 * @since 2019年1月31日
 * @date 2019年1月31日
 */
public class SqlResolve {

    private static final String SUM   = "sum[(].*?[)] as .*? (?=from)|sum[(].*?[)].*?(?=from)";
    private static final String MIN   = "min[(].*?[)] as .*? (?=from)|min[(].*?[)].*?(?=from)";
    private static final String MAX   = "max[(].*?[)] as .*? (?=from)|max[(].*?[)].*?(?=from)";
    private static final String AVG   = "avg[(].*?[)] as .*? (?=from)|avg[(].*?[)].*?(?=from)";
    private static final String COUNT = "count[(].*?[)] as .*? (?=from)|count[(].*?[)].*?(?=from)";

    private String              sql;
    private String[]            sqlUnit;

    public SqlResolve(String sql) {
        // sql预处理
        sql = sql.replace("\r|\n", " ");
        String sqls[] = sql.split(" |\r|\n");
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < sqls.length; i++) {
            String cur = sqls[i];
            if (!StringUtils.isEmpty(cur.trim())) {
                buffer.append(cur.trim());
                if ((i + 1 < sqls.length)
                        && !(sqls[i + 1].startsWith("(") || sqls[i + 1].startsWith(")") || sqls[i + 1].startsWith(","))) {
                    buffer.append(" ");
                }
            }
        }
        sql = buffer.toString();
        this.sql = sql.toLowerCase().trim();
        this.sqlUnit = this.sql.split(" |,");
        for (int i = 0; i < sqlUnit.length; i++) {
            sqlUnit[i] = sqlUnit[i].trim();
        }
    }

    public String getSql() {
        return sql;
    }

    public String[] getSqlUnit() {
        return sqlUnit;
    }

    /**
     * @Description: 获取子查询sql中的聚合函数，只能有一列
     * @since 2019年1月31日
     * @date 2019年1月31日
     * @return key -> function,value -> column
     */
    private Map<String, String> getAggregateFunction(String sql) {
        String aggs[] = { SUM, MIN, MAX, AVG, COUNT };
        Map<String, String> aggMap = new HashMap<>();
        for (int j = 0; j < aggs.length; j++) {
            Pattern pattern = Pattern.compile(aggs[j]);
            Matcher matcher = pattern.matcher(sql.trim());
            while (matcher.find()) {
                String match = matcher.group();
                String col[] = match.split("[(]|[)]|,| ");
                aggMap.put(col[col.length - 1], col[0]);
            }
        }
        return aggMap;
    }

    /**
     * @Description: 获取sql查询参数中的聚合函数
     * @since 2019年2月21日
     * @date 2019年2月21日
     * @return
     */
    public Map<String, String> getAggregateFunction() {
        // 找出子查询，判断子查询中是否有聚合函数，剔除子查询，找出查询参数列表
        String sql = this.sql;
        String reg = "[(]select.*?from.*?where.*?[(].*?[)][)]( as .*?,| as .* | as .*|as .*?,|as .* |as .*)|"
                + "[(]select.*?from.*?where.*?[(].*?[)].*?[)]( as .*?,| as .* | as .*|as .*?,|as .* |as .*)|"
                + "[(]select.*?from.*?where.*?(<|>|=|<=|>=).*?[)]( as .*?,| as .* | as .*|as .*?,|as .* |as .*)";
        Pattern pattern = Pattern.compile(reg);
        Matcher matcher = pattern.matcher(sql.toLowerCase());
        Map<String, String> aggs = new LinkedHashMap<>();
        String creg = "(?<=[(]).*(?=[)])";
        Pattern cpattern = Pattern.compile(creg);
        while (matcher.find()) {
            String csql = matcher.group();
            String arr[] = csql.trim().split(" |,");
            String col = null;
            for (int i = arr.length - 1; i >= 0; i--) {
                if (!StringUtils.isEmpty(arr[i])) {
                    col = arr[i];
                    break;
                }
            }
            String column = col;
            Matcher cmatcher = cpattern.matcher(csql);
            if (cmatcher.find()) {
                Map<String, String> maps = getAggregateFunction(matcher.group());
                if (maps.size() == 1) {
                    maps.forEach((k, v) -> {
                        aggs.put(column, v);
                    });
                } else if (maps.size() > 1) {
                    maps.forEach((k, v) -> {
                        if (column.equals(k)) {
                            aggs.put(column, v);
                        }
                    });
                }
            }
            sql = sql.toLowerCase().replace(csql.trim(), "");
        }
        String qreg = "(?<=select).*(?=from)";
        pattern = Pattern.compile(qreg);
        matcher = pattern.matcher(sql);
        while (matcher.find()) {
            String qcloumn = matcher.group().trim();
            if (!StringUtils.isEmpty(qcloumn)) {
                String[] funs = new String[] { "sum[(].*?[)].*?as.*?(?=,)|sum[(].*?[)].*?as.*?(?= )|sum[(].*?[)]",
                        "min[(].*?[)].*?as.*?(?=,)|min[(].*?[)].*?as.*?(?= )|min[(].*?[)]",
                        "max[(].*?[)].*?as.*?(?=,)|max[(].*?[)].*?as.*?(?= )|max[(].*?[)]",
                        "avg[(].*?[)].*?as.*?(?=,)|avg[(].*?[)].*?as.*?(?= )|avg[(].*?[)]",
                        "count[(].*?[)].*?as.*?(?=,)|count[(].*?[)].*?as.*?(?= )|count[(].*?[)]" };
                for (String freg : funs) {
                    pattern = Pattern.compile(freg);
                    matcher = pattern.matcher(qcloumn);
                    while (matcher.find()) {
                        String agcloumn = matcher.group();
                        String arr[] = agcloumn.split("[(]|[)]| ");
                        aggs.put(arr[arr.length - 1], arr[0]);
                    }
                }
            }
        }
        return aggs;
    }

    /**
     * @Description: 获取sql中的分页参数
     * @since 2019年1月31日
     * @date 2019年1月31日
     * @return start,end
     */
    public Map<String, Integer> getLimit(List<ParameterMapping> mappings, Map<String, Object> paramMap) {
        Map<String, Integer> limits = new LinkedHashMap<>();
        for (int i = sqlUnit.length - 1; i >= 0; i--) {
            if (sqlUnit[i].trim().equals("limit")) {
                int size = mappings.size();
                Object start = paramMap.get(mappings.get(size - 2).getProperty());
                limits.put("start", Integer.parseInt(String.valueOf(start)));
                Object end = paramMap.get(mappings.get(size - 1).getProperty());
                limits.put("end", Integer.parseInt(String.valueOf(end)));
                break;
            } else if ("where".equals(sqlUnit[i])) {
                break;
            }
        }
        return limits;
    }

    /**
     * @Description: 获取sql中的排序参数
     * @since 2019年1月31日
     * @date 2019年1月31日
     * @return
     */
    public Map<String, String> getOrderBy() {
        Map<String, String> orderBys = new LinkedHashMap<>();
        for (int i = sqlUnit.length - 1; i >= 0; i--) {
            if ("by".equals(sqlUnit[i]) && "order".equals(sqlUnit[i - 1])) {
                for (int j = i + 1; j < sqlUnit.length; j++) {
                    if ("limit".equals(sqlUnit[j])) {
                        break;
                    } else if ("".equals(sqlUnit[j].trim())) {
                        continue;
                    }
                    String key = sqlUnit[j];
                    do {
                        orderBys.put(key, sqlUnit[++j]);
                    } while ("".equals(sqlUnit[j].trim()));
                }
            } else if ("where".equals(sqlUnit[i])) {
                break;
            }
        }
        return orderBys;
    }

    /**
     * @Description: 获取分组参数
     * @since 2019年1月31日
     * @date 2019年1月31日
     * @return
     */
    public List<String> getGroupBy() {
        List<String> groupBys = new LinkedList<>();
        for (int i = sqlUnit.length - 1; i >= 0; i--) {
            if ("group".equals(sqlUnit[i]) && "by".equals(sqlUnit[i + 1])) {
                for (int j = i + 2; j < sqlUnit.length; j++) {
                    if ("having".equals(sqlUnit[j]) || ("order".equals(sqlUnit[j]) && "by".equals(sqlUnit[j + 1]))
                            || "limit".equals(sqlUnit[j])) {
                        break;
                    }
                    groupBys.add(sqlUnit[j]);
                }
            } else if ("where".equals(sqlUnit[i])) {
                break;
            }
        }
        return groupBys;
    }

}
