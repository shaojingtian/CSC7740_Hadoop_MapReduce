package record;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.Date;

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

public class AddressMapper extends Mapper<LongWritable, Text, Text, Text> {

  /*
   * The map method runs once for each line of text in the input file.
   * The method receives a key of type LongWritable, a value of type
   * Text, and a Context object.
   */
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    /*
     * Convert the line, which is received as a Text object,
     * to a String object.
     */
    String line = value.toString();

    /*
     * Split the string into a list of 2 strings by " - - \\[".
     * Among which address is the first element,
     * date is the first 20 characters of the second element.
     */
    String splited [] = line.split(" - - \\[");
    if (splited.length > 1) {
        String address = splited[0];
        String date = splited[1].substring(0,20);
  
        /*
         * Call the write method on the Context object to emit a key of ip address
         * and a value of request time from the map method.
         */
        context.write(new Text(address), new Text(date));
    }
  }
}