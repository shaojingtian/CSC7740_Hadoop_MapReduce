package job_wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Pattern;


public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

 
    String line = value.toString();

    /*
     * Eliminate the non-alphabetic words from the result, only assign alphabetic words to keys.
     */
    for (String word : line.split("\\W+")) {
    	
      Pattern p = Pattern.compile ("^[a-zA-Z]*$");
      
      if (word.length() > 0 && p.matcher(word).find()==true) {
    	
        context.write(new Text(word.toLowerCase()), new IntWritable(1));
      }
    }
  }
}