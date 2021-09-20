package assignment2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class EuclidianPartitioner extends Partitioner<Text, DoubleWritable>  {

   @Override
   public int getPartition(Text key, DoubleWritable value, int numPartitions) {
      int val = (int)value.get();
      return val % numPartitions;
   }
}
