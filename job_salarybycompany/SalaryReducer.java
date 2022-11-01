package job_salarybycompany;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SalaryReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
    int sum = 0;
    int counter = 0;
    for (IntWritable value: values) {
    	sum += value.get();
    	counter ++;
    }
    
    /*
     * Calculate the average of mean salary of all positions.
     */
    int average = sum / counter;
	context.write(key, new IntWritable (average));
  }
}