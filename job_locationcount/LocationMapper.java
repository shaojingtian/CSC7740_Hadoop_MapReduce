package job_locationcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Pattern;



public class LocationMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

 
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    
    String line = value.toString();
    String [] line_split = line.split(",");
    
    /*
     * Each record in this txt file has two lines. when split by ",", the first line has 4 elements, the second line has 3 elements - company rating, 
     * city, and state. Check the length of the split line to see if it is the first or second line.
     */     
    if (line_split.length==3) {
    	String city = line_split[1].substring(1);
    	String state = line_split[2].substring(1,3);
        context.write(new Text(city + "," + state), new IntWritable(1));
    }
  }
}