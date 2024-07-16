package bigdata.test02;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * k1 v1
 * 0  1 0 1 3432
 *
 * k2           v2
 * 1 0 1 3432   NullWritable
 */
public class MyPartitionerMapper extends Mapper<LongWritable, Text, Text, NullWritable>  {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        context.write(value, NullWritable.get());
    }
}
