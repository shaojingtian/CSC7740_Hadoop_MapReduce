package record;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/* 
 * To define a reduce function for your MapReduce job, subclass 
 * the Reducer class and override the reduce method.
 * The class definition requires four parameters: 
 *   The data type of the input key (which is the output key type 
 *   from the mapper)
 *   The data type of the input value (which is the output value 
 *   type from the mapper)
 *   The data type of the output key
 *   The data type of the output value
 */   
public class TimeReducer extends Reducer<Text, Text, Text, Text> {

  /*
   * The reduce method runs once for each key received from
   * the shuffle and sort phase of the MapReduce framework.
   * The method receives a key of type Text, a set of values of type
   * Text, and a Context object.
   */
  @Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
	    
	  /* 
	   * Set an variable called last_time and initialize it with a value of date + time that is as early as possible.
	   * For each iteration of request time for each ip address, compare last_time with every request time.
	   * Find the last request time for each ip address.
	   */
	    String last_time = new String ("01/Jan/1900:00:00:00");
	    SimpleDateFormat formatter = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
	
		try {
			Date converted_last_time = formatter.parse(last_time);
		
	        for (Text value: values) {
	    	    String date_time = value.toString();
				Date converted_date_time = formatter.parse(date_time);
				if (converted_date_time.after(converted_last_time)) {
		    	    converted_last_time = converted_date_time;		
		    	}
	        }
	    
	       String last_time_for_address = formatter.format(converted_last_time);
	       /*
			 * Call the write method on the Context object to emit a key of ip address
			 * and a value of the last request time from the reduce method. 
			 */
		   context.write(key, new Text(last_time_for_address));
		   
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}			
	}
}