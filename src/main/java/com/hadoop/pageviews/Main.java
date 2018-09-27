package com.hadoop.pageviews;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 将清洗之后的日志梳理出点击流pageviews模型数据
 *
 * 输入数据是清洗过后的结果数据
 *
 * 区分出每一次会话，给每一次visit（session）增加了session-id（随机uuid）
 * 梳理出每一次会话中所访问的每个页面（请求时间，url，停留时长，以及该页面在这次session中的序号）
 * 保留referral_url，body_bytes_send，useragent
 */
public class Main {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PreProcessBean");

        job.setJarByClass(Main.class);
        job.setMapperClass(PageViewMapper.class);
        job.setReducerClass(PageViewReducer.class);

        //设置 map 阶段的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PreProcessBean.class);

        //设置输出类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        //设置文件的输入输出路径
        FileInputFormat.setInputPaths(job, new Path("C:\\git_projects\\clickLogData"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\git_projects\\output"));

        boolean wait = job.waitForCompletion(true);
        System.exit(wait ? 0 : 1);
    }
}
