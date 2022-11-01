package average;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
    double sum = 0;
    double counter = 0;
    for (IntWritable value: values) {
    	sum += value.get();
    	counter ++;
    }
    double average = sum / counter;
	context.write(key, new DoubleWritable (average));
  }
}