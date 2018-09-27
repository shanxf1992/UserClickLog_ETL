package com.hadoop.pageviews;

import com.hadoop.common.DateTimeParse;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class PageViewReducer extends Reducer<Text, PreProcessBean, NullWritable, Text>{
    private Text val = new Text();

    @Override
    protected void reduce(Text key, Iterable<PreProcessBean> values, Context context) throws IOException, InterruptedException {

        List<PreProcessBean> preProcessBeans = new ArrayList<>();

        // 将迭代器中的数据添加到集合当中
        for (PreProcessBean value : values) {
            preProcessBeans.add(value);
        }

        // 将集合数据根据时间进行排序
        preProcessBeans.sort(new Comparator<PreProcessBean>() {
            @Override
            public int compare(PreProcessBean o1, PreProcessBean o2) {
                try {
                    Date d1 = DateTimeParse.str2Date(o1.getTime_local());
                    Date d2 = DateTimeParse.str2Date(o2.getTime_local());
                    if(d1 == null || d2 == null) return 0;
                    return d1.compareTo(d2);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return 0;
            }
        });

        //设置点 pageView 模型数据
        //就是比较相邻两条记录中的时间差，如果时间差<30分钟，则该两条记录属于同一个session, 否则，就属于不同的session
        int step = 1;
        String session = UUID.randomUUID().toString().replace("-", "");

        //1 如果当前用户只访问一次
        if (preProcessBeans.size() == 1) {
            PreProcessBean processBean = preProcessBeans.get(0);
            val.set(session + "\001" + processBean.getRemote_addr() + "\001" + processBean.getRemote_user() + "\001" + processBean.getTime_local() + "\001"
                    + processBean.getRequest() + "\001" + step + "\001" + (60) + "\001" + processBean.getHttp_referer() + "\001"
                    + processBean.getHttp_user_agent() + "\001" + processBean.getBody_bytes_sent() + processBean.getStatus());

            context.write(NullWritable.get(), val);
            return;
        }

        //2 如果用户访问不止一次, 需要计算其访问步长, 和 访问时间( 根据后期一次记录, 计算前一次记录的时间,和步长)
        for (int i = 1; i < preProcessBeans.size(); i++) {
            // 计算两次访问的时间的间隔
            long timediff = DateTimeParse.datetimeDiff(preProcessBeans.get(i).getTime_local(), preProcessBeans.get(i - 1).getTime_local()); // 秒数
            // 判断时间间隔是否大于 30 分钟, 否则为同一次会话
            if (timediff < 30 * 60) {
                val.set(session + "\001" + preProcessBeans.get(i - 1).getRemote_addr() + "\001" + preProcessBeans.get(i - 1).getRemote_user() + "\001" + preProcessBeans.get(i - 1).getTime_local() + "\001"
                        + preProcessBeans.get(i - 1).getRequest() + "\001" + step + "\001" + (timediff) + "\001" + preProcessBeans.get(i - 1).getHttp_referer() + "\001"
                        + preProcessBeans.get(i - 1).getHttp_user_agent() + "\001" + preProcessBeans.get(i - 1).getBody_bytes_sent() + preProcessBeans.get(i - 1).getStatus());

                context.write(NullWritable.get(), val);
                step++;
            } else {
                // 两次访问时间间隔 超过 30 分钟, 则为两次会话, 设置上一次访问时长为 60 秒(业务指定)
                val.set(session + "\001" + preProcessBeans.get(i - 1).getRemote_addr() + "\001" + preProcessBeans.get(i - 1).getRemote_user() + "\001" + preProcessBeans.get(i - 1).getTime_local() + "\001"
                        + preProcessBeans.get(i - 1).getRequest() + "\001" + step + "\001" + (60) + "\001" + preProcessBeans.get(i - 1).getHttp_referer() + "\001"
                        + preProcessBeans.get(i - 1).getHttp_user_agent() + "\001" + preProcessBeans.get(i - 1).getBody_bytes_sent() + preProcessBeans.get(i - 1).getStatus());

                context.write(NullWritable.get(), val);
                step = 1; // 步长置1 , 重新计算下一次
                session = UUID.randomUUID().toString().replace("-", "");// 重新生成 session
            }

            // 最后一条记录直接输出
            if (i == preProcessBeans.size() - 1) {
                val.set(session + "\001" + preProcessBeans.get(i - 1).getRemote_addr() + "\001" + preProcessBeans.get(i - 1).getRemote_user() + "\001" + preProcessBeans.get(i - 1).getTime_local() + "\001"
                        + preProcessBeans.get(i - 1).getRequest() + "\001" + step + "\001" + (60) + "\001" + preProcessBeans.get(i - 1).getHttp_referer() + "\001"
                        + preProcessBeans.get(i - 1).getHttp_user_agent() + "\001" + preProcessBeans.get(i - 1).getBody_bytes_sent() + preProcessBeans.get(i - 1).getStatus());

                context.write(NullWritable.get(), val);
            }
        }


    }
}
