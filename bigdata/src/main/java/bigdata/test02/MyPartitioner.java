package bigdata.test02;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.awt.geom.Ellipse2D;

/**
 * 1.MapReduce有默认的分区规则，这里我们不用
 * 2.我们需要自定义分区规则
 *   2.1 继承partition类
 *   2.2 重写getPartition方法
 *   2.3 在方法内部对每一个k2，v2 打编号
 */
public class MyPartitioner extends Partitioner<Text, NullWritable> {


    /**
     * @param text  k2
     * @param nullWritable v2
     * @param i   reduce的Task个数，这个参数在JobMain中设置
     * @return int
     */
    public int getPartition(Text text, NullWritable nullWritable, int i) {
        String s = text.toString();
        int i1 = Integer.parseInt(s);
        if (i1 > 10){
            return 0;
        }else {
            return 1;
        }
    }
}
