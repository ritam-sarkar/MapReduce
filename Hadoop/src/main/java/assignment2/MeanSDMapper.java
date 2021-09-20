package assignment2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MeanSDMapper extends Mapper<Object, Text, Text, DoubleWritable> {

   @Override
   public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String data = value.toString();
      String[] field = data.split(" ", -1);
      double sum = 0;
      double sumSquare = 0;
      double count = 0;
      if (field.length > 0) {
         for (String d : field){
            double dataPoint = Double.parseDouble(d);
            sum += dataPoint;
            sumSquare += Math.pow(dataPoint,2);
            count ++;
         }
      }
      Text sumKey = new Text();
      Text sumSquareKey = new Text();
      Text countKey = new Text();
      DoubleWritable sumVal = new DoubleWritable();
      DoubleWritable sumSquareVal = new DoubleWritable();
      DoubleWritable countVal = new DoubleWritable();

      // Sum
      sumKey.set("SUM");
      sumVal.set(sum);

      // Sum square
      sumSquareKey.set("SUMSQUARE");
      sumSquareVal.set(sumSquare);

      // Count
      countKey.set("COUNT");
      countVal.set(count);

      context.write(sumKey, sumVal);
      context.write(sumSquareKey, sumSquareVal);
      context.write(countKey, countVal);

   }
}
