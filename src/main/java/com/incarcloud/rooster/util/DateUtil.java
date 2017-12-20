package com.incarcloud.rooster.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 时间转换工具类
 */
public class DateUtil {

    public static Date date = null;

    public static DateFormat dateFormat = null;

    public static Calendar calendar = null;

    /**
     * 日期类型转字符串
     *
     * @param date   时间
     * @param format 日期字符串格式(例如：yyyy-MM-dd HH:mm:ss)
     * @return 时间字符串
     * @author Teemol
     */
    public static String getTimeStr(Date date, String format) {
        SimpleDateFormat outFormat = new SimpleDateFormat(format);
        String timeStr = outFormat.format(date);

        return timeStr;
    }

    /**
     * 日期类型转字符串(返回格式：yyyy-MM-dd HH:mm:ss)
     *
     * @param date 时间
     * @return 时间字符串
     * @author Teemol
     */
    public static String getTimeStr(Date date) {
        SimpleDateFormat outFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String timeStr = outFormat.format(date);

        return timeStr;
    }

    /**
     * 日期字符串转时间
     *
     * @param dateStr 日期字符串
     * @param format  日期字符串格式(例如：yyyy-MM-dd HH:mm:ss)
     * @return 日期
     * @author Teemol
     */
    public static Date getDate(String dateStr, String format) {
        if (dateStr != null && !dateStr.equals("")) {
            DateFormat dateFormat = new SimpleDateFormat(format);
            try {
                return dateFormat.parse(dateStr);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

        return null;
    }

    /**
     * 日期字符串转时间(返回格式：yyyy-MM-dd HH:mm:ss)
     *
     * @param dateStr 日期字符串
     * @return 日期
     * @author Teemol
     */
    public static Date getDate(String dateStr) {
        if (dateStr != null && !dateStr.equals("")) {
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            try {
                return dateFormat.parse(dateStr);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

        return null;
    }

    /**
     * 获取当天初始时间 (返回格式：yyyy-MM-dd 00:00:00)
     *
     * @param date 时间
     * @return 初始时间
     * @author Teemol
     */
    public static Date getInitialTime(Date date) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd 00:00:00");
        String dateStr = dateFormat.format(date);
        try {
            return dateFormat.parse(dateStr);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 获取当天最后时间 (返回格式：yyyy-MM-dd 23:59:59)
     *
     * @param date 时间
     * @return 最后时间
     * @author Teemol
     */
    public static Date getTerminalTime(Date date) {

        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String dateStr = dateFormat.format(date);
        dateStr = dateStr + " 23:59:59";

        dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            return dateFormat.parse(dateStr);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 获取整点时间 (返回格式：yyyy-MM-dd HH:00:00)
     *
     * @param date 时间
     * @return 整点时间
     * @author Teemol
     */
    public static Date getHourTime(Date date) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:00:00");
        String dateStr = dateFormat.format(date);
        try {
            return dateFormat.parse(dateStr);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 在时间上增加小时
     *
     * @param date  时间
     * @param hours 小时数
     * @return 增加后的时间
     * @author Teemol
     */
    public static Date plusHours(Date date, int hours) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.HOUR, hours);

        return calendar.getTime();
    }

    /**
     * 在时间上增加天
     *
     * @param date 时间
     * @param days 天数
     * @return 增加后的时间
     * @author Teemol
     */
    public static Date plusDays(Date date, int days) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DATE, days);

        return calendar.getTime();
    }

    /**
     * 在时间上增加月
     *
     * @param date   时间
     * @param months 月数
     * @return 增加后的时间
     * @author Teemol
     */
    public static Date plusMonths(Date date, int months) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.MONTH, months);

        return calendar.getTime();
    }

    /**
     * 在时间上增加年
     *
     * @param date  时间
     * @param years 年数
     * @return 增加后的时间
     * @author Teemol
     */
    public static Date plusYears(Date date, int years) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.YEAR, years);

        return calendar.getTime();
    }

    /**
     * 获取时间为当天第几个小时
     *
     * @param date 时间
     * @return 小时数
     * @author Teemol
     */
    public static int getHour(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);

        return calendar.get(Calendar.HOUR_OF_DAY);
    }

    /**
     * 计算两个时间相差多少天
     *
     * @param dateBegin 开始时间
     * @param dateEnd   结束时间
     * @return 相差天数
     * @author Teemol
     */
    public static int getDayDiff(Date dateBegin, Date dateEnd) {
        int day = 0;
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            String str1 = sdf.format(dateBegin);
            String str2 = sdf.format(dateEnd);
            long number = sdf.parse(str2).getTime() - sdf.parse(str1).getTime();
            day = (int) (number / (1000 * 60 * 60 * 24));
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return day;
    }

    /**
     * 计算两个时间相差多少小时
     *
     * @param dateBegin 开始时间
     * @param dateEnd   结束时间
     * @return 相差小时数
     * @author Teemol
     */
    public static int getHourDiff(Date dateBegin, Date dateEnd) {
        Long begin = dateBegin.getTime();
        Long end = dateEnd.getTime();

        return (int) ((end - begin) / (1000 * 60 * 60));
    }

}
