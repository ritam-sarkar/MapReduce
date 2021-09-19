package assignment2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CustomMapper extends Mapper<Object, Text, Text, DoubleWritable> {
   private DoubleWritable doubleWritable = new DoubleWritable();
   private Text key = new Text();

   @Override
   public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String data = value.toString();
      String[] field = data.split(",", -1);
      if (field.length > 0) {
         for (String d : field){
            double dataPoint = Double.parseDouble(d);
            doubleWritable.set(dataPoint);
            this.key.set("Dummy");
            context.write(this.key, doubleWritable);
         }
      }
   }
}
