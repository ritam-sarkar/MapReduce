package assignment2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CustomReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
   private DoubleWritable sdResult = new DoubleWritable();
   private DoubleWritable meanResult = new DoubleWritable();
   private List<Double> dataPoints = new ArrayList<>();

   public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
         throws IOException, InterruptedException {
      double sum = 0;
      int count = 0;
      double meanDiff = 0;

      for (DoubleWritable val : values) {
         double data = val.get();
         dataPoints.add(data);
         sum += data;
         count++;
      }
      double mean = sum / count;
      for (Double val : dataPoints) {
         meanDiff += Math.pow(val - mean, 2);
      }
      double sd = Math.sqrt(meanDiff / (count - 1));
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
