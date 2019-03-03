package com.frank.sharding.util;

import java.lang.reflect.Field;

/**
 * @description 反射工具给传入对象设定值
 * @author fengsheng
 * @since 2019年2月1日
 * @date 2019年2月1日
 */
public class ReflectUtil {

    /**
     * @Description: 获取指定对象的指定属性
     * @since 2019年2月1日
     * @date 2019年2月1日
     * @param obj
     * @param fieldName
     * @return
     */
    public static Object getFieldValue(Object obj, String fieldName) {
        Object result = null;
        Field field = ReflectUtil.getField(obj, fieldName);
        if (field != null) {
            field.setAccessible(true);
            try {
                result = field.get(obj);
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    /**
     * @Description: 获取指定对象里面的指定属性对象
     * @since 2019年2月1日
     * @date 2019年2月1日
     * @param obj
     * @param fieldName
     * @return
     */
    private static Field getField(Object obj, String fieldName) {
        Field field = null;
        for (Class<?> clazz = obj.getClass(); clazz != Object.class; clazz = clazz.getSuperclass()) {
            try {
                field = clazz.getDeclaredField(fieldName);
                break;
            } catch (NoSuchFieldException e) {
                // do nothing
            }
        }
        return field;
    }

    /**
     * @Description: 设置指定对象的指定属性值
     * @since 2019年2月1日
     * @date 2019年2月1日
     * @param obj
     * @param fieldName
     * @param fieldValue
     */
    public static void setFieldValue(Object obj, String fieldName, String fieldValue) {
        Field field = ReflectUtil.getField(obj, fieldName);
        if (field != null) {
            try {
                field.setAccessible(true);
                field.set(obj, fieldValue);
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
    }

}
