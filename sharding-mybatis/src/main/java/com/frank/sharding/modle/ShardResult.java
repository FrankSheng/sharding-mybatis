package com.frank.sharding.modle;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.springframework.jdbc.core.RowMapper;

/**
 * @description 分表结果处理类
 * @author fengsheng
 * @since 2019年1月7日
 * @date 2019年1月7日
 */
public class ShardResult implements RowMapper<Map<String, Object>>{
    
    @Override
    public Map<String, Object> mapRow(ResultSet rs, int rowNum) throws SQLException {
        Map<String, Object> dataMap = new HashMap<String, Object>();
        ResultSetMetaData data = rs.getMetaData();
        int count = data.getColumnCount();
        for (int i = 1; i <= count; i++) {
            Object value = rs.getObject(i);
            String name = data.getColumnName(i);
            dataMap.put(name, value);
        }
        return dataMap;
    }
    
}
