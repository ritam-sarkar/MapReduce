package assignment2;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// The driver program for mapreduce job.
public class FindMeanSD extends Configured implements Tool {

   public static void main(String[] args) throws Exception {
      FileUtils.deleteDirectory(new File(args[1]));
      int res = ToolRunner.run(new Configuration(), new FindMeanSD(), args);
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {

      Configuration conf = this.getConf();

      // Create job
      Job job = Job.getInstance(conf, "Mean and Standard Deviation Job");
      job.setJarByClass(FindMeanSD.class);

      job.setMapperClass(CustomMapper.class);
      job.setReducerClass(CustomReducer.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(DoubleWritable.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      job.setInputFormatClass(TextInputFormat.class);

      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      job.setOutputFormatClass(TextOutputFormat.class);

      return job.waitForCompletion(true) ? 0 : 1;
   }
}