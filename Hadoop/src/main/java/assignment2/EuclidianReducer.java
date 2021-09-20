package assignment2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EuclidianReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

   private static final double X_CORDINATE = 5;
   private static final double Y_CORDINATE = 8;

   public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

      int i= 0;
      double currentX = 0;
      double currentY = 0;
      for (DoubleWritable value : values){
         if(i ==0) {
            currentX = value.get();
         } else if(i== 1){
            currentY = value.get();
         }
         i++;
      }
      double sumOfSquare  =  Math.pow(currentX - X_CORDINATE, 2) + Math.pow(currentY - Y_CORDINATE, 2);
      double distance = Math.sqrt(sumOfSquare);
      DoubleWritable euclidianDistance = new DoubleWritable();
      euclidianDistance.set(distance);
      context.write(key , euclidianDistance);
   }
}
