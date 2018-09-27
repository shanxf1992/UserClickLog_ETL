package com.hadoop.visits;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 输入数据：pageviews模型结果数据
 * 从pageviews模型结果数据中进一步梳理出visit模型
 * sessionid  start-time   out-time   start-page   out-page   pagecounts  ......
 */
public class VisitsMapper extends Mapper<LongWritable, Text, Text, PageViewsBean> {

    private PageViewsBean pageViewsBean= new PageViewsBean();
    private Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    }

    /**
     * 根据 pageviews 模型数据, 按照 session 为 key 输出
     * @param key 偏移量
     * @param value pageviews 模型数据
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\001");
        pageViewsBean.set(fields[0], fields[1], fields[2], fields[3], fields[4], Integer.parseInt(fields[5]), fields[6], fields[7], fields[8], fields[9]);
        k.set(pageViewsBean.getSession());
        context.write(k, pageViewsBean);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
