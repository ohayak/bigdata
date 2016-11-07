import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TP3Reducer2 extends Reducer<LongWritable, Text, LongWritable, LongWritable>{

	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int i = 0;
		for (Text str: values) {
			i++;
		}
		context.write(new LongWritable((long) Math.pow(10, key.get())), new LongWritable(i));
	}
}
