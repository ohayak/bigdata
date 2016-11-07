import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TP3Mapper2 extends Mapper<Object, Text, LongWritable, AvgWritable>{
	private static Logger LOGGER = Logger.getLogger(TP3Mapper1.class.getName());

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String tokens[] = value.toString().split("\\s+|\\t");
		long keyOut = (int) Math.floor(Math.log10(Integer.parseInt(tokens[1])));
		try {
			int pop = Integer.parseInt(tokens[1]);
			AvgWritable w = new AvgWritable(pop, 1, pop, pop);
			context.write(new LongWritable(keyOut), w);
			context.getCounter("WCP", "nb_cities").increment(1);
		}
		catch (NumberFormatException e) {
			LOGGER.info("ville invalide: populatio="+tokens[4]);
		}
	}
}

