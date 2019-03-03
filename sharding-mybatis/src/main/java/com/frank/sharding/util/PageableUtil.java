package com.frank.sharding.util;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

/**
 * @description 内存分页工具，分页数据最多100万
 * @author fengsheng
 * @since 2018年5月28日
 * @date 2018年5月28日
 */
public class PageableUtil {

    /**
     * @Description:  内存分页传入数据不要超过100万
     * @since 2019年2月19日
     * @date 2019年2月19日
     * @param start 起始位置
     * @param end 结束位置
     * @param data
     * @return
     * @throws Exception
     */
    public static <T> List<T> page(int start, int end, List<T> data) throws Exception {
        List<T> pageData = new LinkedList<T>();
        int total = data.size();
        if (start > total) {
            return pageData;
        }
        if (end > total) {
            end = total;
        }
        pageData.addAll(data.subList(start, end));// 左闭右开[start,end)
        return pageData;
    }

    /**
     * @Description: 对list集合排序,jdk官方默认是升序，是基于： < return -1;= return 0;> return 1
     * @since 2019年2月1日
     * @date 2019年2月1日
     * @param property
     *            排序那列
     * @param isAsc
     *            是否是升序
     * @param data
     *            集合数据
     * @return
     * @throws Exception
     */
    public static List<Object> sort(String property, boolean isAsc, List<Object> data) throws Exception {
        Collections.sort(data, new Comparator<Object>() {
            @Override
            public int compare(Object o1, Object o2) {
                if (ObjectUtil.isBasicType(o1) || ObjectUtil.isString(o1)) {
                    return 0;
                } else {
                    Object v1 = ReflectUtil.getFieldValue(o1, property);
                    Object v2 = ReflectUtil.getFieldValue(o2, property);
                    try {
                        if (isAsc) {
                            return comparable(v1, v2);
                        } else {
                            return comparable(v2, v1);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                return 0;
            }
        });
        return data;
    }

    /**
     * @Description: 比较两个值的大小,升序，是基于： < return -1;= return 0;> return
     *               1,v1/v2的类型必须一致
     * @since 2019年2月18日
     * @date 2019年2月18日
     * @param v1
     * @param v2
     * @return
     * @throws Exception
     */
    public static int comparable(Object v1, Object v2) throws Exception {
        if (!v1.getClass().getName().equals(v2.getClass().getName()))
            throw new Exception("The types of V1 and V2 are inconsistent");
        if (ObjectUtil.isString(v1) && ObjectUtil.isString(v2)) {
            String rv1 = (String) v1;
            String rv2 = (String) v2;
            return rv1.compareTo(rv2);
        } else if (ObjectUtil.isBasicType(v1) && ObjectUtil.isBasicType(v2)) {
            if (v1 instanceof Long) {
                Long rv1 = (Long) v1;
                Long rv2 = (Long) v2;
                return rv1.compareTo(rv2);
            } else if (v1 instanceof Integer) {
                Integer rv1 = (Integer) v1;
                Integer rv2 = (Integer) v2;
                return rv1.compareTo(rv2);
            } else if (v1 instanceof Double) {
                Double rv1 = (Double) v1;
                Double rv2 = (Double) v2;
                return rv1.compareTo(rv2);
            } else if (v1 instanceof Float) {
                Float rv1 = (Float) v1;
                Float rv2 = (Float) v2;
                return rv1.compareTo(rv2);
            } else if (v1 instanceof Short) {
                Short rv1 = (Short) v1;
                Short rv2 = (Short) v2;
                return rv1.compareTo(rv2);
            } else if (v1 instanceof Boolean) {
                Boolean rv1 = (Boolean) v1;
                Boolean rv2 = (Boolean) v2;
                return rv1.compareTo(rv2);
            } else if (v1 instanceof Character) {
                Character rv1 = (Character) v1;
                Character rv2 = (Character) v2;
                return rv1.compareTo(rv2);
            } else if (v1 instanceof Byte) {
                Byte rv1 = (Byte) v1;
                Byte rv2 = (Byte) v2;
                return rv1.compareTo(rv2);
            }
        }
        return 0;
    }
}
