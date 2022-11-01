package job_salarybycompany;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CompanyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String line = value.toString();
    
    String low_end_string = " ";
    String high_end_string = " ";
    int low_end = 0;
    int high_end = 0;
    
    String [] list = line.split(",");
    
    /*
     * Each record in this txt file has two lines. when split by ",", the first line has 4 elements, the second line has 3 elements.
     * Check the length of the split line to see if it is the first or second line.
     */    
    if (list.length == 4) {
       String [] salary_range = list[1].split("\\W+");
       
       /*
        * Extract the numeric letter from the string of salary estimate, and cast them to integer.
        */
       if (salary_range.length>2) {
           low_end_string = salary_range[1].replaceAll("[^0-9]", "");
           high_end_string = salary_range[2].replaceAll("[^0-9]", "");
       }
       
       if (low_end_string != " " && low_end_string.length() != 0 ) {
           low_end = Integer.parseInt(low_end_string);    	   
       }
       
       if (high_end_string != " " && high_end_string.length() != 0) {
    	   high_end = Integer.parseInt(high_end_string);
       }
       
       /*
        * The salary estimate is a range, calculate the mean of it.
        */
       int mean = (low_end + high_end)/2;
       context.write(new Text (list[3].substring(1)), new IntWritable (mean));		    		
    }
  }
}