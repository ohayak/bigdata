import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TP3Mapper1 extends Mapper<Object, Text, Text, LongWritable>{
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String tokens[] = value.toString().split(",");
		String keyOut = tokens[0]+tokens[1];
		keyOut = keyOut.replaceAll("\\s+", "_").toLowerCase();
		try {
			context.write(new Text(keyOut), new LongWritable(Integer.parseInt(tokens[4])));
			context.getCounter("WCP", "nb_cities").increment(1);
		}
		catch (NumberFormatException e) {
			e.printStackTrace();
		}
	}
}

