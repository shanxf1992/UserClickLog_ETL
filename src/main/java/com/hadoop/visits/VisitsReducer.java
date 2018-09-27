package com.hadoop.visits;

import com.hadoop.common.DateTimeParse;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

public class VisitsReducer extends Reducer<Text, PageViewsBean, NullWritable, Text> {

    private List<PageViewsBean> pageViewsBeans= new ArrayList<>();
    private Text val = new Text();
    private VisitsBean visitsBean = new VisitsBean();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    }

    @Override
    protected void reduce(Text key, Iterable<PageViewsBean> values, Context context) throws IOException, InterruptedException {
        // 将迭代器数据添加到集合中, 便于排序
        for (PageViewsBean pageViewsBean : values) {
            pageViewsBeans.add(pageViewsBean);
        }

        // 将每个session 对应的不同 pageview 数据按照时间进行排序
        pageViewsBeans.sort(new Comparator<PageViewsBean>() {
            @Override
            public int compare(PageViewsBean o1, PageViewsBean o2) {
                Date date1 = DateTimeParse.str2Date(o1.getTimestr());
                Date date2 = DateTimeParse.str2Date(o2.getTimestr());
                if (date1 == null || date2 == null) return 0;
                return date1.compareTo(date2);
            }
        });

        // 根据每个 session 对应的访问记录生成 visits 模型数据
        // 取visit的首记录
        visitsBean.setInPage(pageViewsBeans.get(0).getRequest());
        visitsBean.setInTime(pageViewsBeans.get(0).getTimestr());
        // 取visit的尾记录
        visitsBean.setOutPage(pageViewsBeans.get(pageViewsBeans.size() - 1).getRequest());
        visitsBean.setOutTime(pageViewsBeans.get(pageViewsBeans.size() - 1).getTimestr());
        // visit访问的页面数
        visitsBean.setPageVisits(pageViewsBeans.size());
        // 来访者的ip
        visitsBean.setRemote_addr(pageViewsBeans.get(0).getRemote_addr());
        // 本次visit的referal
        visitsBean.setReferal(pageViewsBeans.get(0).getReferal());
        visitsBean.setSession(key.toString());

        //清空集合
        pageViewsBeans.clear();
        val.set(visitsBean.toString());
        context.write(NullWritable.get(), val);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
