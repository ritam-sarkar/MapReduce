package test;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AlphaMapper extends Mapper<Object, Text, Text, LongWritable> {
   private final static LongWritable one = new LongWritable(1);
   private Text character = new Text();

   @Override
   public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String v = value.toString();
      for (int i = 0; i < v.length(); i++) {
         character.set(v.substring(i, i + 1));
         context.write(character, one);
      }
   }
}
