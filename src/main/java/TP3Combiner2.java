import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class TP3Combiner2 extends Reducer<LongWritable, LongWritable, LongWritable, AvgWritable>{

	public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		int sum=0;
		int count=0;
		int max=0;
		int min=0;
		for (LongWritable itr: values) {
			count++;
			sum = (int) ((long)sum+itr.get());
		    max = (long)max > itr.get() ? max : (int)itr.get();
		    min = (long)min < itr.get() ? min : (int)itr.get();
		}
		
		context.write(new LongWritable((long) Math.pow(10, key.get())), new AvgWritable(sum,count,max,min));
	}
}