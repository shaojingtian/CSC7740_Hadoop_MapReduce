package hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBasePut2 {

	public static void main(String[] args) throws IOException {
		Configuration config = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(config);
		Table table = connection.getTable(TableName.valueOf("courses"));

		try {
			Put put1 = new Put(Bytes.toBytes("CS3501"));
		    Put put2 = new Put(Bytes.toBytes("CS4740"));
		    Put put3 = new Put(Bytes.toBytes("CS4998"));
		    Put put4 = new Put(Bytes.toBytes("CS7402"));
			put1.addImmutable(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("comp.org."));
			put1.addImmutable(Bytes.toBytes("info"), Bytes.toBytes("credit"), Bytes.toBytes("3"));
			put2.addImmutable(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("big data"));
			put2.addImmutable(Bytes.toBytes("info"), Bytes.toBytes("credit"), Bytes.toBytes("3"));
			put2.addImmutable(Bytes.toBytes("info"), Bytes.toBytes("since"), Bytes.toBytes("2015"));
			put2.addImmutable(Bytes.toBytes("instructors"), Bytes.toBytes("2015S"), Bytes.toBytes("Qang"));
			put2.addImmutable(Bytes.toBytes("instructors"), Bytes.toBytes("2020S"), Bytes.toBytes("Lee"));
			put3.addImmutable(Bytes.toBytes("info"), Bytes.toBytes("credit"), Bytes.toBytes("1"));
			put4.addImmutable(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("DB"));
			put4.addImmutable(Bytes.toBytes("info"), Bytes.toBytes("credit"), Bytes.toBytes("3"));
			put4.addImmutable(Bytes.toBytes("instructors"), Bytes.toBytes("2020S"), Bytes.toBytes("TBA"));
			table.put(put1);
			table.put(put2);
			table.put(put3);
			table.put(put4);
		} finally {
			table.close();
			connection.close();
		}

	}

}
