package job_findword;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Pattern;
import java.util.Arrays;



public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
	/*
	 * Assign the phrase you want to find to the String variable phrase_to_find.
	 */
    String phrase_to_find = new String ("Machine Learning").toLowerCase();
    String [] list_phrase_to_find = phrase_to_find.split("\\W+");
    int length_of_phrase = list_phrase_to_find.length;
    
    String line = value.toString().toLowerCase();
    String [] list = line.split("\\W+");
    
    /*
     * Compare two arrays of words (two phrases) to find how many times the phrase appears in the file.
     */
    for (int i=0; i<list.length-length_of_phrase+1; i++) {
    	if (Arrays.equals(Arrays.copyOfRange(list, i, i+length_of_phrase), list_phrase_to_find)==true) {
            context.write(new Text(phrase_to_find), new IntWritable(1));
         }
    }
  }
}