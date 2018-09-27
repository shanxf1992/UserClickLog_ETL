package com.hadoop.preprocess;

import com.hadoop.common.DateTimeParse;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * 对用户点击日志数据进行预处理, 过滤出无效的数据, 如静态资源, 重新指定分隔符
 194.237.142.21 - - [18/Sep/2013:06:49:18 +0000] "GET /wp-content/uploads/2013/07/rstudio-git3.png HTTP/1.1" 304 0 "-" "Mozilla/4.0 (compatible;)"
 183.49.46.228 - - [18/Sep/2013:06:49:23 +0000] "-" 400 0 "-" "-"
 163.177.71.12 - - [18/Sep/2013:06:49:33 +0000] "HEAD / HTTP/1.1" 200 20 "-" "DNSPod-Monitor/1.0"
 ..
 */
public class LogPreProcessMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Set<String> filterUrl = new HashSet<>();
    private ClickLogBean clickLogBean = new ClickLogBean();
    //初始化key value
    private Text k = new Text("");
    private NullWritable v = NullWritable.get();

    /**
     * 初始化有效的 访问 url , 过滤掉一些 对于图片, js等静态资源的访问
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        filterUrl.add("/about");
        filterUrl.add("/black-ip-list/");
        filterUrl.add("/cassandra-clustor/");
        filterUrl.add("/finance-rhive-repurchase/");
        filterUrl.add("/hadoop-family-roadmap/");
        filterUrl.add("/hadoop-hive-intro/");
        filterUrl.add("/hadoop-zookeeper-intro/");
        filterUrl.add("/hadoop-mahout-roadmap/");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split(" ");
        //1 过滤掉分割后字段长度小于 11 的记录( 过滤无效请求 )
        if (fields.length < 11) return;

        //2 设置字段
        clickLogBean.setRemote_addr(fields[0]); //设置ip
        clickLogBean.setRemote_user(fields[1]); //记录客户端用户名称,忽略属性"-"
        clickLogBean.setRequest(fields[6]); // 记录请求的url与http协议
        clickLogBean.setStatus(fields[8]); // 记录请求状态；成功是200
        clickLogBean.setBody_bytes_sent(fields[9]); // 记录发送给客户端文件主体内容大小
        clickLogBean.setHttp_referer(fields[10]); // 用来记录从那个页面链接访问过来的
        //解析时间
        String time = DateTimeParse.parseDate(fields[3]);
        clickLogBean.setTime_local(time == null ? "invaild time" : time);

        //3 设置 useragent(浏览器信息) 字段
        if (fields.length == 12) {
            clickLogBean.setHttp_user_agent(fields[11]);
        } else {
            StringBuilder sb = new StringBuilder();
            for (int i = 11; i < fields.length ; i++) {
                sb.append(fields[i]).append(" ");
            }
            clickLogBean.setHttp_user_agent(sb.toString());
        }

        //4 设置点击数据的合法性, 即状态是否有效, 是否是静态资源访问
        if (Integer.parseInt(clickLogBean.getStatus()) >= 400 || "invaild time".equals(clickLogBean.getTime_local()) || !filterUrl.contains(fields[6])) {
            clickLogBean.setValid(false);
        } else {
            clickLogBean.setValid(true);
        }

        k.set(clickLogBean.toString());
        context.write(k, v);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
