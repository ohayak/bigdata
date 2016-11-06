import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TP3Reducer extends Reducer<Text, LongWritable, Text, LongWritable>{

	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		long max = 0;
		for (LongWritable i: values) {
			if (i.get() > max) {
				max = i.get();
			}
		}
		context.write(key, new LongWritable(max));
		context.getCounter("WCP", "nb_pop").increment(1);
		context.getCounter("WCP", "total_pop").increment(max);
	}
}
