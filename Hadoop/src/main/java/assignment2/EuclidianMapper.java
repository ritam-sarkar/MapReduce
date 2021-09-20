package assignment2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EuclidianMapper extends Mapper<Object, Text, Text, DoubleWritable> {

   @Override
   public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String data = value.toString();
      String[] field = data.split(" ", -1);
      if (field.length > 0) {
         for (String rawPoint : field){
            String[] p = rawPoint.split(",", -1);
            DoubleWritable x = new DoubleWritable();
            DoubleWritable y = new DoubleWritable();
            x.set(Double.parseDouble(p[0]));
            y.set(Double.parseDouble(p[1]));
            Text point = new Text();
            point.set(rawPoint);
            context.write(point, x);
            context.write(point, y);
         }
      }

   }
}
