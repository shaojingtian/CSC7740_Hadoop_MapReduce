package tool;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* 
 * To define a map function for your MapReduce job, subclass 
 * the Mapper class and override the map method.
 * The class definition requires four parameters: 
 *   The data type of the input key
 *   The data type of the input value
 *   The data type of the output key (which is the input key type 
 *   for the reducer)
 *   The data type of the output value (which is the input value 
 *   type for the reducer)
 */

public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
  private boolean caseSensitive = false;
  
  @Override
  protected void setup(Context context )
	  throws IOException, InterruptedException {
	Configuration conf = context.getConfiguration();
	caseSensitive = conf.getBoolean("caseSensitive", false);
	super.setup(context);
  }

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String line = value.toString();
    
    for (String word : line.split("\\W+")) {
      if (word.length() > 0) {
    	  if (caseSensitive == true) {
               context.write(new Text(word), new IntWritable(1));
           }
    	  else  {
    		   context.write(new Text(word.toLowerCase()), new IntWritable(1));
    	  }
      }
    }
  }
}