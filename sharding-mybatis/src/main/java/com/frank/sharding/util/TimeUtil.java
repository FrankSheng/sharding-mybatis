package com.frank.sharding.util;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;


public class TimeUtil {

    private static SimpleDateFormat sdf  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
    private static SimpleDateFormat sdf3 = new SimpleDateFormat("yyyyMMddHHmmss");
    private static SimpleDateFormat sdf4 = new SimpleDateFormat("yyyy-MM-dd HH:mm");

    /**
     * 格式化时间 yyyy-MM-dd HH:mm:ss
     * 
     * @param time
     * @return
     */
    public static String format(Timestamp time) {
        if (time == null)
            return "";
        return sdf.format(time);
    }

    /**
     * 格式化时间为 ：yyyy-MM-dd HH:mm:ss
     * 
     * @param time
     * @return
     */
    public static String format(Date time) {
        if (time == null)
            return "";
        return sdf.format(time);
    }

    /**
     * 格式化时间为 ：yyyy-MM-dd HH:mm:ss 线程安全
     * 
     * @param time
     * @return
     */
    public static String format6(Date time) {
        if (time == null)
            return "";
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return format.format(time);
    }

    /**
     * @Description: 格式化成 yyyy-MM-dd HH:mm
     * @since 2018年6月12日
     * @date 2018年6月12日
     * @param time
     * @return
     */
    public static String format4(Date time) {
        if (time == null)
            return "";
        return sdf4.format(time);
    }

    /**
     * 格式化时间为：yyyy-MM-dd
     * 
     * @param date
     * @return
     */
    public static String format2(Timestamp date) {
        if (date == null)
            return "";
        return sdf2.format(date);
    }

    /**
     * 格式化时间为：yyyy-MM-dd
     * 
     * @param date
     * @return
     */
    public static String format2(Date date) {
        if (date == null)
            return "";
        return sdf2.format(date);
    }

    /**
     * 格式化时间为：yyyyMMddHHmmss
     * 
     * @param date
     * @return
     */
    public static String format3(Timestamp date) {
        if (date == null)
            return "";
        return sdf3.format(date);
    }

    /**
     * 格式化时间为：yyyyMMddHHmmss
     * 
     * @param date
     * @return
     */
    public static String format3(Date date) {
        if (date == null)
            return "";
        return sdf3.format(date);
    }

    /**
     * 取得日期与当前日期天数差
     * 
     * @throws ParseException
     */
    public static long datepoor(Date d) throws ParseException {
        String now = sdf2.format(new Date());
        Date n = sdf2.parse(now);
        return (d.getTime() - n.getTime()) / (1000 * 60 * 60 * 24);
    }

    /**
     * 判断是否是日期格式
     * 
     * @param s
     * @return
     */
    public static boolean isDate(String s) {
        if (s == null)
            return true;
        boolean res = true;
        try {
            sdf2.parse(s);
        } catch (Exception e) {
            res = false;
        }
        return res;
    }

    /**
     * @Description: 将字符串格式时间（yyyy-mm-dd hh:mm）转换成date
     * @since 2018年4月28日
     * @date 2018年4月28日
     * @param date
     * @return
     * @throws ParseException
     */
    public static Date strToDate(String date) throws ParseException {
        return sdf4.parse(date);
    }

    /**
     * @Description: 将字符串格式时间（yyyy-mm-dd）转换成date
     * @since 2018年11月20日
     * @date 2018年11月20日
     * @param date
     * @return
     * @throws ParseException
     */
    public static Date strToDate1(String date) throws ParseException {
        return sdf2.parse(date);
    }

    /**
     * @Description: 将字符串格式时间（yyyy-mm-dd hh:mm:ss）转换成date
     * @since 2018年7月17日
     * @date 2018年7月17日
     * @param date
     * @return
     * @throws ParseException
     */
    public static Date strToDate2(String date) throws ParseException {
        // SimpleDateFormat非线程安全，所以需要每个方法调用的时候创建自己的对象
        SimpleDateFormat sdf5 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf5.parse(date);
    }

    /**
     * @Description: 将字符串格式时间（ hh:mm:ss）转换成date
     * @since 2018年11月13日
     * @date 2018年11月13日
     * @param date
     * @return
     * @throws ParseException
     */
    public static Date strToDate3(String date) throws ParseException {
        // SimpleDateFormat非线程安全，所以需要每个方法调用的时候创建自己的对象
        SimpleDateFormat sdf6 = new SimpleDateFormat("HH:mm:ss");
        return sdf6.parse(date);
    }

    /**
     * 获取当天开始的时间，包含时分秒 yyyy-mm-dd 00:00:00
     * 
     * @return
     */
    public static Date getTodayStartTime() {
        Calendar todayStart = Calendar.getInstance();
        todayStart.set(Calendar.HOUR_OF_DAY, 0);
        todayStart.set(Calendar.MINUTE, 0);
        todayStart.set(Calendar.SECOND, 0);
        return todayStart.getTime();
    }

    /**
     * 获取当天结束的时间，包含时、分、秒 yyyy-mm-dd 23:59:59
     * 
     * @return
     */
    public static Date getTodayEndTime() {
        Calendar todayEnd = Calendar.getInstance();
        todayEnd.set(Calendar.HOUR_OF_DAY, 23);
        todayEnd.set(Calendar.MINUTE, 59);
        todayEnd.set(Calendar.SECOND, 59);
        return todayEnd.getTime();
    }

    /**
     * 获取当前时间
     * 
     * @return
     */
    public static Date getNowDate() {
        Calendar now = Calendar.getInstance();
        return now.getTime();
    }

    public static Date getTime(int hour, int minute, int second) {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, hour);
        calendar.set(Calendar.MINUTE, minute);
        calendar.set(Calendar.SECOND, second);
        return calendar.getTime();
    }
    /**
     * @Description: 判断是字符串是不是指定 yyyy-MM-dd HH:mm:ss时间格式
     * @since 2019年1月28日
     * @date 2019年1月28日
     * @param str
     * @return 不是时间格式返回null
     */
    public static Date isValidDate(String str) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            format.setLenient(false);
            format.parse(str);
        } catch (ParseException e) {
            return null;
        }
        return format.getCalendar().getTime();
    }
}
