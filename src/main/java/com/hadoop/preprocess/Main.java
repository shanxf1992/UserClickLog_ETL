package com.hadoop.preprocess;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.xml.soap.Text;


public class Main {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "LogPreProcess");

            job.setJarByClass(Main.class);
            job.setMapperClass(LogPreProcessMapper.class);

            //设置mapper 输出数据的key value类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            //设置数据输入输出路径
            FileInputFormat.setInputPaths(job, new Path("C:\\git_projects\\access.log.fensi"));
            FileOutputFormat.setOutputPath(job, new Path("C:\\git_projects\\output"));

            // 设置reducer 数为 0
            job.setNumReduceTasks(0);
            boolean completion = job.waitForCompletion(true);
            System.exit(completion ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
