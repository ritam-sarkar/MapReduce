package assignment2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MeanSDReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

   private double totalSum = 0;
   private double totalSumSquare = 0;
   private double totalCount = 0;

   @Override
   public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
         throws IOException, InterruptedException {

      DoubleWritable sdResult = new DoubleWritable();
      DoubleWritable meanResult = new DoubleWritable();

      String keyAsString = key.toString();

      for (DoubleWritable val : values) {
         double data = val.get();
         if (keyAsString.equals("SUM")) {
            totalSum += data;
         } else if (keyAsString.equals("SUMSQUARE")) {
            totalSumSquare += data;
         } else if (keyAsString.equals("COUNT")) {
            totalCount += data;
         }
      }

      if (totalSum > 0 && totalCount > 0 && totalSumSquare > 0) {
         double mean = totalSum / totalCount;
         double variance = (totalSumSquare / totalCount) - Math.pow(mean, 2);
         double sd = Math.sqrt(variance);

         sdResult.set(sd);
         meanResult.set(mean);
         Text sdKey = new Text();
         sdKey.set("Standard Deviation");
         Text meanKey = new Text();
         meanKey.set("Mean");
         context.write(sdKey, sdResult);
         context.write(meanKey, meanResult);
      }
   }
}
