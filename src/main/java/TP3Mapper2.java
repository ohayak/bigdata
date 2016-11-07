import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TP3Mapper2 extends Mapper<Object, Text, LongWritable, Text>{
	private static Logger LOGGER = Logger.getLogger(TP3Mapper1.class.getName());

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String tokens[] = value.toString().split("\\s+|\\t");
		long keyOut = (int) Math.floor(Math.log10(Integer.parseInt(tokens[1])));
		try {
			context.write(new LongWritable(keyOut), new Text(tokens[0]));
			context.getCounter("WCP", "nb_cities").increment(1);
		}
		catch (NumberFormatException e) {
			LOGGER.info("ville invalide: populatio="+tokens[4]);
		}
	}
}

