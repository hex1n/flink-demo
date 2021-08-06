package com.dlwlrma.flink.api.demo;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public final class ChinaTimeUtils {

    public static final DateFormat chinaDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    static {
        chinaDateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
    }

    public static final long _That_Day;
    static {
        Calendar thatDay = Calendar.getInstance(TimeZone.getTimeZone("Asia/Shanghai"));
        thatDay.set(2020, Calendar.JULY, 6, 0, 0, 0);  // 这一天是星期一
        thatDay.set(Calendar.MILLISECOND, 0);
        _That_Day = thatDay.getTimeInMillis();
    }
    public static final long aHour = 3600L * 1000;
    public static final long aDay = 24L * aHour;

    public static long getDate(int year, int month, int day) {
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("Asia/Shanghai"));
        calendar.set(year, month - 1, day, 0, 0, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTimeInMillis();
    }

    /**
     * 得到参数day相距_That_Day距离天数
     * @param day
     * @return
     */
    public static int getRelativeDay(long day) {
        return (int) ((day - _That_Day) / aDay);
    }

    /**
     * 比较两个时间是否在同一天
     * @param day1
     * @param day2
     * @return
     */
    public static int compareDay(long day1, long day2) {
        return Long.compare(getRelativeDay(day1), getRelativeDay(day2));
    }

    /**
     * 星期1 -> 1, 星期二 -> 2 ... 星期日 -> 7
     * @param day
     * @return
     */
    public static int getDayOfWeek(long day) {
        return getRelativeDay(day) % 7 + 1;
    }

    /**
     * 减去天数得到新的时间，且时分秒都清0
     * @param nDay
     * @return
     */
    public static long subDaysAndClearHMS(long tm, int nDay) {
        return _That_Day + getRelativeDay(tm) * aDay - nDay * aDay;
    }

    /**
     * 获取今天年月日，时分秒为0
     * @return
     */
    public static long getToday() {
        return subDaysAndClearHMS(System.currentTimeMillis(), 0);
    }

    /**
     * 获取昨天年月日，时分秒为0
     * @return
     */
    public static long getYesterday() {
        return subDaysAndClearHMS(System.currentTimeMillis(), 1);
    }

    public static long getHour(long tm) {
        return (tm - _That_Day) % (aDay) / aHour;
    }

    /**
     * 得到tm所在星期的星期一，且时分秒都清0
     * @param tm
     * @return
     */
    public static long getMonday(long tm) {
        int relativeDay = getRelativeDay(tm);
        return _That_Day + relativeDay * aDay - relativeDay % 7 * aDay;
    }

    public static int dayDiff(long day1, long day2) {
        return getRelativeDay(day1) - getRelativeDay(day2);
    }

    public static String format(long tm) {
        return chinaDateFormat.format(tm);
    }

    public static String format(Date tm) {
        return chinaDateFormat.format(tm);
    }
}
