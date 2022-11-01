package index;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IndexMapper extends Mapper<Text, Text, NullWritable, Put> {

  @Override
  public void map(Text key, Text value, Context context) throws IOException,
      InterruptedException {
  
    String line = value.toString();
    Set<String> set = new HashSet<String>();
  
    String line_num = key.toString();
  
    for (String word : line.split("\\W+")) { 
        if (word.length() > 0) 
          set.add(word);
    }

    for (String uniqueword : set){
	    Put put = new Put(Bytes.toBytes(uniqueword));
	    byte [] columnFamily = Bytes.toBytes("cf");
	    byte [] qualifier = Bytes.toBytes(line_num);
	    byte [] value_table = Bytes.toBytes(" ");
	    put.addImmutable(columnFamily, qualifier, value_table);
	   
        context.write(null, put);
    }
	  
    
  }
}