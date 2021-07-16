package org.n0nb0at.phoneflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class PhoneFlowStatistic {

    public PhoneFlowStatistic() {
        // constructor
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: wordCount <in> <out>");
            System.exit(2);
        }

        Job job = createJob(conf);
        FileInputFormat.setMaxInputSplitSize(job, 512);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        boolean flag = job.waitForCompletion(true);
        System.out.println("Succeed! " + flag);
        System.exit(flag ? 0 : 1);
        System.out.println();
    }

    private static Job createJob(Configuration conf) throws IOException {
        Job job = Job.getInstance(conf, "phone flow counter");
        job.setJarByClass(PhoneFlowStatistic.class);
        job.setMapperClass(PhoneFlowMapper.class);
        job.setReducerClass(PhoneFlowReducer.class);
        job.setOutputKeyClass(PhoneBean.class);
        job.setOutputValueClass(FlowBean.class);
        job.setNumReduceTasks(3);
        return job;
    }

    public static class PhoneFlowMapper extends Mapper<Object, Text, PhoneBean, FlowBean> {
        PhoneBean phone = new PhoneBean();
        FlowBean flowData;

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 输入：0 时间戳、1 电话号码、2 基站的物理地址、3 访问网址的ip、4 网站域名、
            // 5 数据包、6 接包数、7 上行/传流量、8 下行/载流量、9 响应码
            String line = value.toString();
            String[] val = line.split("\t");

            // 手机号 第1列
            phone.setPhoneNo(val[1]);

            // 上下行流量 第7、8列
            long upFlow = Long.parseLong(val[7]);
            long downFlow = Long.parseLong(val[8]);
            flowData = new FlowBean(upFlow, downFlow);

            context.write(phone, flowData);
        }
    }

    public static class PhoneFlowReducer extends Reducer<PhoneBean, FlowBean, PhoneBean, FlowBean> {
        FlowBean flowData;
        @Override
        protected void reduce(PhoneBean key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            long upFlowSum = 0;
            long downFlowSum = 0;
            for (FlowBean flowBean : values) {
                upFlowSum += flowBean.getUpFlow();
                downFlowSum += flowBean.getDownFlow();
            }
            flowData = new FlowBean(upFlowSum, downFlowSum);

            context.write(key, flowData);
        }
    }
}
