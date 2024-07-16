package bigdata.test02;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyPartitionerReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
}
