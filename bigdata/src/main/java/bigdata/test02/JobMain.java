package bigdata.test02;

import bigdata.hellotest.WordCountMapper;
import bigdata.hellotest.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class JobMain {


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //打印日志
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "my_partitioner");
        // 指定job所在的jar包
        job.setJarByClass(bigdata.hellotest.JobMain.class);

        //制定源文件的读取方式和源文件的路径
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("hdfs://node1:8020/input/wordcount"));

        job.setMapperClass(MyPartitionerMapper.class);//指定Mapper k2
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //指定分区
        job.setPartitionerClass(MyPartitioner.class);
        //设置reduce个数
        job.setNumReduceTasks(2);

        job.setReducerClass(MyPartitionerReducer.class);//指定Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //指定输出方式，和输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("hdfs://node1:8020/output/wordcount"));

        //将job提交到yarn集群
        boolean b = job.waitForCompletion(true);

        //退出
        System.exit(b?0:1);
    }
}
