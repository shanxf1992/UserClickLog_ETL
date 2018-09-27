package com.hadoop.pageviews;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * map 端
 * 输入: 清洗后的数据
 * 输出: 封住后的 pageviewbean
 *
 * 区分出每一次会话，给每一次visit（session）增加了session-id（随机uuid）
 * 梳理出每一次会话中所访问的每个页面（请求时间，url，停留时长，以及该页面在这次session中的序号）
 * 保留referral_url，body_bytes_send，useragent
 */
public class PageViewMapper extends Mapper<LongWritable, Text, Text, PreProcessBean> {

    private Text k = new Text("");
    private PreProcessBean v = new PreProcessBean();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\001");
        if ("false".equals(fields[0])) return;

        //将切分出来的各字段set到weblogbean中
        v.set(true, fields[1], fields[2], fields[3], fields[4], fields[5], fields[6], fields[7], fields[8]);

        //用ip地址来标识用户
        k.set(v.getRemote_addr());
        context.write(k, v);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
