package com.hadoop.visits;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Main {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);

            job.setJarByClass(Main.class);
            job.setMapperClass(VisitsMapper.class);
            job.setReducerClass(VisitsReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(PageViewsBean.class);

            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.setInputPaths(job, new Path("C:\\git_projects\\PageViewData"));
            FileOutputFormat.setOutputPath(job, new Path("C:\\git_projects\\output"));


            boolean wait = job.waitForCompletion(true);
            System.exit(wait ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
