package tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/* 
 * MapReduce jobs are typically implemented by using a driver class.
 * The purpose of a driver class is to set up the configuration for the
 * MapReduce job and to run the job.
 * Typical requirements for a driver class include configuring the input
 * and output data formats, configuring the map and reduce classes,
 * and specifying intermediate data formats.
 * 
 * The following is the code for the driver class:
 */
public class WordCount extends Configured implements Tool{

  public static void main(String[] args) throws Exception {

	int exitCode = ToolRunner.run(new Configuration(), new WordCount(), args);
	System.exit(exitCode);
  }
   
  @Override
  public int run (String[] args) throws Exception {
      if (args.length != 2) {
           System.out.printf(
                 "Usage: WordCount <input dir> <output dir>\n");
           System.exit(-1);
       }
       
       
      Job job = Job.getInstance(getConf(), "word count");
      job.setJarByClass(WordCount.class);
    
      FileInputFormat.setInputPaths(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
  
      job.setMapperClass(WordMapper.class);
      job.setReducerClass(SumReducer.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

    
      boolean success = job.waitForCompletion(true);
      return success ? 0 : 1;
  }
}