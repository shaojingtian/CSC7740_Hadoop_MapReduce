package job_companyrating;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;

import java.util.regex.Pattern;





public class RatingMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

  
    String line = value.toString();
    String [] line_list = line.split(","); 
    double rating = 0.0;
    /*
     * every record in this txt file has two lines. first line has 4 elements, and second line has 3 elements.
     * If the length equals 4, that means it is the first line of each record which consists of company rating and company name.
     */
    if (line_list.length == 4) {
         String company = line_list[3].substring(1);
         String rating_string = line_list[2];
        
         rating = Double.parseDouble(rating_string);
       	 
         context.write(new Text(company), new DoubleWritable(rating));
     }
  }
}