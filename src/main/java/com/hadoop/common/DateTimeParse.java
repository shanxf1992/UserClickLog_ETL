package com.hadoop.common;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * 解析日期格式工具类
 */
public class DateTimeParse {
    private static SimpleDateFormat df1 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
    private static SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);

    /**
     * 解析日期
     *
     * @param time 格式 : [18/Sep/2013:06:49:18
     * @return 解析后格式 : 2013-9-18 06:49:18
     */
    public static String parseDate(String time) {

        try {
            String datetime = time.substring(1);
            Date date = df1.parse(datetime);
            return df2.format(date);
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }


    /**
     * 将字符串类型的 日期, 转化为 Date 类型
     * @param time 格式: 2013-9-18 06:49:18
     * @return
     */
    public static Date str2Date(String time) {
        try {
            return df2.parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 计算两个时间之间的 差值
     * @param time1 格式: 2013-9-18 06:49:18
     * @param time2 格式: 2013-9-18 06:49:18
     * @return 返回单位 秒
     */
    public static long datetimeDiff(String time1, String time2) {
        Date date1 = str2Date(time1);
        Date date2 = str2Date(time2);

        return (date1.getTime() - date2.getTime()) / 1000;
    }

}
