package job_companyrating;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.PropertyConfigurator;


public class CompanyRating {

  public static void main(String[] args) throws Exception {

  
    if (args.length != 2) {
      System.out.printf(
          "Usage: WordCount <input dir> <output dir>\n");
      System.exit(-1);
    }

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    
   
    job.setJarByClass(CompanyRating.class);
    
    
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    

    job.setMapperClass(RatingMapper.class);
    job.setReducerClass(RatingReducer.class);

   
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);

    
    boolean success = job.waitForCompletion(true);
    System.exit(success ? 0 : 1);
  }
}