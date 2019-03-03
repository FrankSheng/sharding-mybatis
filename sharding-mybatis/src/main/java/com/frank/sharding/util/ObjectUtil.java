package com.frank.sharding.util;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

public class ObjectUtil {

    /**
     * @Description: 判断一个对象是否属于基本数据类型
     * @since 2019年1月24日
     * @date 2019年1月24日
     * @param obj
     * @return
     */
    public static boolean isBasicType(Object obj) {
        if (obj instanceof Long || obj instanceof Integer || obj instanceof Double || obj instanceof Float
                || obj instanceof Short || obj instanceof Boolean || obj instanceof Character || obj instanceof Byte) {
            return true;
        }
        return false;
    }

    /**
     * @Description: 判断一个对象是否属于字符串类型
     * @since 2019年1月24日
     * @date 2019年1月24日
     * @param obj
     * @return
     */
    public static boolean isString(Object obj) {
        if (obj instanceof String) {
            return true;
        }
        return false;
    }

    /**
     * @Description: 将对象转换成map集合,若父类中与子类相同的属性，取子类属性值
     * @since 2019年1月31日
     * @date 2019年1月31日
     * @param object
     * @return
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> objectToMap(Object object) throws Exception {
        Map<String, Object> map = new LinkedHashMap<String, Object>();
        if (object instanceof Array) {
            throw new Exception("Array type is not supported!");
        } else if (object instanceof Collection) {
            throw new Exception("Collection type is not supported!");
        } else if (isBasicType(object) || isString(object)) {
            throw new Exception(object.getClass().getName() + " type is not supported!");
        } else if (object instanceof Map) {
            Map<Object, Object> oMap = (Map<Object, Object>) object;
            oMap.forEach((key, value) -> {
                if (isBasicType(key) || isString(key)) {
                    map.put(String.valueOf(key), value);
                }
            });
        } else {
            Class<?> clazz = object.getClass();
            while (clazz != Object.class) {
                Field[] filds = clazz.getDeclaredFields();
                for (Field field : filds) {
                    String property = field.getName();
                    String me = property.substring(0, 1).toUpperCase();
                    String getter = "get" + me + property.substring(1);
                    Method method = clazz.getMethod(getter, new Class[] {});
                    Object obj = method.invoke(object, new Object[] {});
                    if (null == map.get(property)) {
                        map.put(property, obj);
                    }
                }
                clazz = clazz.getSuperclass();
            }
        }
        return map;
    }
}
