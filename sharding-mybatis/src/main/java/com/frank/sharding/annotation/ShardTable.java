package com.frank.sharding.annotation;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import com.frank.sharding.rule.ShardRuleType;
/**
 * @description 分表注解器，目前只支持按照日期分表，分为当前表（最近一个月数据），历史表（最近2-3月数据），归档表（3个月以前的数据）
 * @author fengsheng
 * @since 2019年1月8日
 * @date 2019年1月8日
 *
 */
@Documented
@Retention(RUNTIME)
@Target(TYPE)
public @interface ShardTable {
        
        /**
         * @Description: 指定表名 
         * @since 2019年1月8日
         * @date 2019年1月8日
         * @return
         */
	public String table();

	/**
	 * @Description: 指定列 
	 * @since 2019年1月8日
	 * @date 2019年1月8日
	 * @return
	 */
	public String column();

	/**
	 * @Description: 分表规则 ,默认按照日期拆分
	 * @since 2019年1月8日
	 * @date 2019年1月8日
	 * @return
	 */
	public int rule() default ShardRuleType.DATE;
	
	
}
