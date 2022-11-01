package job_companyrating;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

 
public class RatingReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {


  @Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
      double sum = 0;
      int counter = 0;
      for (DoubleWritable value: values) {
    	  sum+=value.get();
    	  counter++;
      }
      double average = sum/counter;
	  context.write(key, new DoubleWritable(average));
	}
}