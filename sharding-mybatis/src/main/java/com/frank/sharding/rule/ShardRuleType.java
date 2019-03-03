package com.frank.sharding.rule;
/**
 * 分表规则
 * @author Administrator
 *
 */
public final class ShardRuleType {
	/**
	 * 按照日期拆分
	 */
	public final static int DATE = 1;
	/**
	 * 按照总数拆分
	 */
	public final static int TOTAL = 2;
	/**
	 * 奇数、偶数拆分
	 */
	public final static int PARITY = 3;
}
